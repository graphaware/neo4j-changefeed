/*
 * Copyright (c) 2014 GraphAware
 *
 * This file is part of GraphAware.
 *
 * GraphAware is free software: you can redistribute it and/or modify it under the terms of
 * the GNU General Public License as published by the Free Software Foundation, either
 * version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 *  without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU General Public License for more details. You should have received a copy of
 * the GNU General Public License along with this program.  If not, see
 * <http://www.gnu.org/licenses/>.
 */

package com.graphaware.module.changefeed;

import org.neo4j.graphdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static com.graphaware.common.util.IterableUtils.getSingleOrNull;
import static com.graphaware.module.changefeed.Labels._GA_ChangeSet;
import static com.graphaware.module.changefeed.Properties.*;
import static com.graphaware.module.changefeed.Relationships._GA_CHANGEFEED_NEXT_CHANGE;
import static com.graphaware.module.changefeed.Relationships._GA_CHANGEFEED_OLDEST_CHANGE;
import static org.neo4j.graphdb.Direction.INCOMING;
import static org.neo4j.graphdb.Direction.OUTGOING;
import static org.neo4j.tooling.GlobalGraphOperations.at;

/**
 * {@link ChangeWriter} that keeps the changes stored in the graph.
 */
public class GraphChangeWriter implements ChangeWriter {

    private static final Logger LOG = LoggerFactory.getLogger(GraphChangeWriter.class);

    private static final int PRUNE_WHEN_MAX_EXCEEDED_BY = 10;

    private final GraphDatabaseService database;

    private AtomicInteger sequence = null;
    private Node root;

    /**
     * Construct a new repository.
     *
     * @param database in which to store the changes.
     */
    public GraphChangeWriter(GraphDatabaseService database) {
        this.database = database;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void initialize() {
        root = getOrCreateRoot();
        initializeSequence();
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public void recordChanges(Set<String> changes) {
        try (Transaction tx = database.beginTx()) {
            tx.acquireWriteLock(getRoot());

            ChangeSet changeSet = new ChangeSet(sequence.incrementAndGet()); //TODO might this result in holes if a runtime exception is thrown at the end of this module or any other
            changeSet.addChanges(changes);

            Node changeNode = database.createNode(_GA_ChangeSet);
            changeNode.setProperty(SEQUENCE, changeSet.getSequence());
            changeNode.setProperty(TIMESTAMP, changeSet.getTimestamp());
            changeNode.setProperty(CHANGES, changeSet.getChangesAsArray());

            Relationship firstChangeRel = getRoot().getSingleRelationship(Relationships._GA_CHANGEFEED_NEXT_CHANGE, Direction.OUTGOING);
            if (firstChangeRel == null) { //First changeSet recorded, create an _GA_CHANGEFEED_OLDEST_CHANGE relation from the root to it
                getRoot().createRelationshipTo(changeNode, Relationships._GA_CHANGEFEED_OLDEST_CHANGE);
            } else {
                Node firstChange = firstChangeRel.getEndNode();
                tx.acquireWriteLock(firstChange);
                changeNode.createRelationshipTo(firstChange, Relationships._GA_CHANGEFEED_NEXT_CHANGE);
                firstChangeRel.delete();
            }

            getRoot().createRelationshipTo(changeNode, Relationships._GA_CHANGEFEED_NEXT_CHANGE);

            tx.success();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void pruneChanges(int keep) {
        try (Transaction tx = database.beginTx()) {
            Relationship oldestChangeRel = getRoot().getSingleRelationship(_GA_CHANGEFEED_OLDEST_CHANGE, OUTGOING);
            if (oldestChangeRel != null) {
                Node oldestNode = oldestChangeRel.getEndNode();
                Node newestNode = getRoot().getSingleRelationship(_GA_CHANGEFEED_NEXT_CHANGE, OUTGOING).getEndNode();
                if (newestNode != null) {
                    long highSequence = (long) newestNode.getProperty(SEQUENCE);
                    long lowSequence = (long) oldestNode.getProperty(SEQUENCE);
                    long nodesToDelete = ((highSequence - lowSequence) + 1) - keep;
                    Node newOldestNode = null;
                    if (nodesToDelete >= PRUNE_WHEN_MAX_EXCEEDED_BY) {
                        LOG.info("Preparing to prune change feed by deleting {} nodes", nodesToDelete);
                        getRoot().getSingleRelationship(_GA_CHANGEFEED_OLDEST_CHANGE, OUTGOING).delete();
                        while (nodesToDelete > 0) {
                            Relationship rel = oldestNode.getSingleRelationship(_GA_CHANGEFEED_NEXT_CHANGE, INCOMING);
                            newOldestNode = rel.getStartNode();
                            rel.delete();
                            oldestNode.delete();
                            oldestNode = newOldestNode;
                            nodesToDelete--;
                        }
                        getRoot().createRelationshipTo(newOldestNode, _GA_CHANGEFEED_OLDEST_CHANGE);
                        LOG.info("ChangeFeed pruning complete");
                    }
                }
            }
            tx.success();
        }
    }

    /**
     * Get or create the root of the change feed.
     *
     * @return root.
     */
    private Node getOrCreateRoot() {
        Node root;

        try (Transaction tx = database.beginTx()) {
            root = getSingleOrNull(at(database).getAllNodesWithLabel(Labels._GA_ChangeFeed));
            if (root == null) {
                LOG.info("Creating the ChangeFeed Root");
                root = database.createNode(Labels._GA_ChangeFeed);
                //Create a unique constraint on sequence
                database.schema().constraintFor(Labels._GA_ChangeSet).assertPropertyIsUnique(Properties.SEQUENCE);
            }
            tx.success();
        }

        return root;
    }

    /**
     * Get the root.
     *
     * @return root, will never be null.
     */
    private Node getRoot() {
        if (root == null) {
            throw new IllegalStateException("There is not ChangeFeed Root! This is a bug. It looks like the start() method hasn't been called.");
        }
        return root;
    }

    /**
     * Initialize the sequence to the last used number. No need to synchronize, called from constructor,
     * thus in a single thread.
     */
    private void initializeSequence() {
        int startSequence = 0;

        try (Transaction tx = database.beginTx()) {
            Relationship nextRel = getRoot().getSingleRelationship(_GA_CHANGEFEED_NEXT_CHANGE, OUTGOING);
            if (nextRel != null) {
                startSequence = (Integer) nextRel.getEndNode().getProperty(SEQUENCE);
            }
            tx.success();
        }

        sequence = new AtomicInteger(startSequence);
    }
}
