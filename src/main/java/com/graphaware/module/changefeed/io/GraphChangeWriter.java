/*
 * Copyright (c) 2013-2016 GraphAware
 *
 * This file is part of the GraphAware Framework.
 *
 * GraphAware Framework is free software: you can redistribute it and/or modify it under the terms of
 * the GNU General Public License as published by the Free Software Foundation, either
 * version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU General Public License for more details. You should have received a copy of
 * the GNU General Public License along with this program.  If not, see
 * <http://www.gnu.org/licenses/>.
 */

package com.graphaware.module.changefeed.io;

import com.graphaware.common.uuid.EaioUuidGenerator;
import com.graphaware.common.uuid.UuidGenerator;
import com.graphaware.module.changefeed.domain.ChangeSet;
import com.graphaware.module.changefeed.domain.Labels;
import com.graphaware.module.changefeed.domain.Relationships;
import org.neo4j.graphdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

import static com.graphaware.common.util.IterableUtils.getSingleOrNull;
import static com.graphaware.module.changefeed.domain.Labels._GA_ChangeSet;
import static com.graphaware.module.changefeed.domain.Properties.*;
import static com.graphaware.module.changefeed.domain.Relationships._GA_CHANGEFEED_NEXT_CHANGE;
import static com.graphaware.module.changefeed.domain.Relationships._GA_CHANGEFEED_OLDEST_CHANGE;
import static org.neo4j.graphdb.Direction.INCOMING;
import static org.neo4j.graphdb.Direction.OUTGOING;

/**
 * {@link ChangeWriter} that keeps the changes stored in the graph.
 */
public class GraphChangeWriter implements ChangeWriter {

    private static final Logger LOG = LoggerFactory.getLogger(GraphChangeWriter.class);
    private final UuidGenerator uuidGenerator = new EaioUuidGenerator();

    private final GraphDatabaseService database;
    private final String moduleId;

    private Node root;

    /**
     * Construct a new writer.
     *
     * @param database in which to store the changes.
     * @param moduleId ID of the module storing changes.
     */
    public GraphChangeWriter(GraphDatabaseService database, String moduleId) {
        this.database = database;
        this.moduleId = moduleId;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void initialize() {
        root = getOrCreateRoot();
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public void recordChanges(Set<String> changes) {
            ChangeSet changeSet = new ChangeSet(uuidGenerator.generateUuid());
            changeSet.addChanges(changes);
            recordChanges(changeSet);
    }

    /**
     * Record (persist) a set of changes.
     *
     * @param changeSet to record.
     */
    protected void recordChanges(ChangeSet changeSet) {
        try (Transaction tx = database.beginTx()) {
            tx.acquireWriteLock(getRoot());

            Node changeNode = database.createNode(_GA_ChangeSet);
            changeNode.setProperty(UUID, changeSet.getUuid());
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
    public void pruneChanges(int keep, int mustBeExceededBy) {
        try (Transaction tx = database.beginTx()) {
            tx.acquireWriteLock(getRoot());
            Relationship oldestChangeRel = getRoot().getSingleRelationship(_GA_CHANGEFEED_OLDEST_CHANGE, OUTGOING);
            if (oldestChangeRel != null) {
                Node oldestNode = oldestChangeRel.getEndNode();
                Node newestNode = getRoot().getSingleRelationship(_GA_CHANGEFEED_NEXT_CHANGE, OUTGOING).getEndNode();
                if (newestNode != null) {
                    int changeCount = 1;
                    Node lastNodeToKeep = newestNode;
                    Relationship nextRel = lastNodeToKeep.getSingleRelationship(_GA_CHANGEFEED_NEXT_CHANGE, OUTGOING);

                    while (changeCount < keep && nextRel != null) {
                        lastNodeToKeep = nextRel.getEndNode();
                        changeCount++;
                        nextRel = lastNodeToKeep.getSingleRelationship(_GA_CHANGEFEED_NEXT_CHANGE, OUTGOING);
                    }

                    if (changeCount < keep) {
                        LOG.debug("Nothing to prune");
                        tx.success();
                        return;
                    }

                    //Now check if there are more changes than the pruneWhenExceeded limit
                    int exceededCount = 0;
                    Relationship nextExceededByRel = nextRel;
                    while (exceededCount < mustBeExceededBy && nextExceededByRel != null) {
                        nextExceededByRel = nextExceededByRel.getEndNode().getSingleRelationship(_GA_CHANGEFEED_NEXT_CHANGE, OUTGOING);
                        exceededCount++;
                    }

                    if (exceededCount < mustBeExceededBy) {
                        LOG.debug("pruneWhenExceeded limit not exceeded, nothing to prune");
                        tx.success();
                        return;
                    }


                    LOG.debug("Preparing to prune change feed");
                    if (nextRel != null) {
                        nextRel.delete();
                        oldestChangeRel.delete();
                        getRoot().createRelationshipTo(lastNodeToKeep, _GA_CHANGEFEED_OLDEST_CHANGE);
                        Relationship previousChange = oldestNode.getSingleRelationship(_GA_CHANGEFEED_NEXT_CHANGE, INCOMING);
                        while (previousChange != null) {
                            Node newOldestNode = previousChange.getStartNode();
                            previousChange.delete();
                            oldestNode.delete();
                            previousChange = newOldestNode.getSingleRelationship(_GA_CHANGEFEED_NEXT_CHANGE, INCOMING);
                            oldestNode = newOldestNode;
                        }
                        oldestNode.delete();
                    }
                    LOG.debug("ChangeFeed pruning complete");
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
            root = getSingleOrNull(database.findNodesByLabelAndProperty(Labels._GA_ChangeFeed, MODULE_ID, moduleId));
            if (root == null) {
                LOG.info("Creating the ChangeFeed Root for Module ID " + moduleId);
                root = database.createNode(Labels._GA_ChangeFeed);
                root.setProperty(MODULE_ID, moduleId);
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
    public Node getRoot() {
        if (root == null) {
            throw new IllegalStateException("There is no ChangeFeed Root for Module ID " + moduleId + "! This is a bug.");
        }
        return root;
    }

}
