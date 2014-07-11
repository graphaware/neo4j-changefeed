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

import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static com.graphaware.common.util.IterableUtils.getSingleOrNull;
import static com.graphaware.module.changefeed.Labels.*;
import static com.graphaware.module.changefeed.Properties.*;
import static com.graphaware.module.changefeed.Relationships.GA_CHANGEFEED_NEXT_CHANGE;
import static com.graphaware.module.changefeed.Relationships.GA_CHANGEFEED_OLDEST_CHANGE;
import static org.neo4j.graphdb.Direction.INCOMING;
import static org.neo4j.graphdb.Direction.OUTGOING;
import static org.neo4j.tooling.GlobalGraphOperations.at;

/**
 * {@link ChangeRepository} that keeps the changes stored in the graph.
 */
public class GraphChangeRepository implements ChangeRepository {

    private static final Logger LOG = LoggerFactory.getLogger(GraphChangeRepository.class);

    private static final String CHANGES_SINCE_QUERY = "match (startChange:_GA_ChangeSet {sequence: {sequence}}) with startChange match (startChange)<-[:GA_CHANGEFEED_NEXT_CHANGE*..]-(change:_GA_ChangeSet) return change order by change.sequence desc";

    private final GraphDatabaseService database;
    private final ExecutionEngine executionEngine;

    private AtomicInteger sequence = null;
    private Node root;

    /**
     * Construct a new repository.
     *
     * @param database in which to store the changes.
     */
    public GraphChangeRepository(GraphDatabaseService database) {
        this.database = database;
        executionEngine = new ExecutionEngine(database);
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
    public List<ChangeSet> getAllChanges() {
        return getChanges("match (root:_GA_ChangeFeed)-[:GA_CHANGEFEED_NEXT_CHANGE*.." + Integer.MAX_VALUE + "]->(change) return change", Collections.<String, Object>emptyMap());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<ChangeSet> getNumberOfChanges(int limit) {
        return getChanges("match (root:_GA_ChangeFeed)-[:GA_CHANGEFEED_NEXT_CHANGE*.." + limit + "]->(change) return change", Collections.<String, Object>emptyMap());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<ChangeSet> getChangesSince(int since) {
        Map<String, Object> params = new HashMap<>();
        params.put(SEQUENCE, since);
        return getChanges(CHANGES_SINCE_QUERY, params);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<ChangeSet> getNumberOfChangesSince(int since, int limit) {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    private List<ChangeSet> getChanges(String query, Map<String, Object> params) {
        try {
            return doGetChanges(query, params);
        } catch (Exception e) {
            LOG.error("Could not fetch changeFeed on first attempt", e);
            try {
                return doGetChanges(query, params);
            } catch (Exception e2) {     //We hope not to reach here
                LOG.error("Could not fetch changeFeed on second attempt", e2);
                throw e2;
            }
        }

    }

    /**
     * Get a list of changes from the graph.
     *
     * @param query  to get changes.
     * @param params for the query, may not be null (but can be empty).
     * @return List of {@link ChangeSet}, latest change first.
     */
    private List<ChangeSet> doGetChanges(String query, Map<String, Object> params) {
        List<ChangeSet> changefeed = new ArrayList<>();

        ExecutionResult result;
        try (Transaction tx = database.beginTx()) {
            result = executionEngine.execute(query, params);
            Iterator<Node> changeNodes = result.columnAs("change");
            while (changeNodes.hasNext()) {
                Node changeNode = changeNodes.next();
                ChangeSet changeSet = new ChangeSet((long) changeNode.getProperty(SEQUENCE), (long) changeNode.getProperty(TIMESTAMP));
                changeSet.addChanges((String[]) changeNode.getProperty(CHANGES));
                changefeed.add(changeSet);
            }
            tx.success();
        }

        return changefeed;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void recordChanges(Set<String> changes) {
        ChangeSet changeSet = new ChangeSet(sequence.incrementAndGet()); //TODO might this result in holes if a runtime exception is thrown at the end of this module or any other
        changeSet.addChanges(changes);
        recordChange(changeSet);
    }

    private void recordChange(ChangeSet changeSet) {
        try (Transaction tx = database.beginTx()) {

            Node changeNode = database.createNode(_GA_ChangeSet);
            changeNode.setProperty(SEQUENCE, changeSet.getSequence());
            changeNode.setProperty(TIMESTAMP, changeSet.getTimestamp());
            changeNode.setProperty(CHANGES, changeSet.getChangesAsArray());

            tx.acquireWriteLock(getRoot());

            Relationship firstChangeRel = getRoot().getSingleRelationship(Relationships.GA_CHANGEFEED_NEXT_CHANGE, Direction.OUTGOING);
            if (firstChangeRel == null) { //First changeSet recorded, create an GA_CHANGEFEED_OLDEST_CHANGE relation from the root to it
                getRoot().createRelationshipTo(changeNode, Relationships.GA_CHANGEFEED_OLDEST_CHANGE);
            } else {
                Node firstChange = firstChangeRel.getEndNode();
                tx.acquireWriteLock(firstChange);
                changeNode.createRelationshipTo(firstChange, Relationships.GA_CHANGEFEED_NEXT_CHANGE);
                firstChangeRel.delete();
            }

            getRoot().createRelationshipTo(changeNode, Relationships.GA_CHANGEFEED_NEXT_CHANGE);

            tx.success();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void pruneChanges(int keep) {
        final int MAX_FEED_LENGTH_EXCEEDED = 3;

        Relationship oldestChangeRel = getRoot().getSingleRelationship(GA_CHANGEFEED_OLDEST_CHANGE, OUTGOING);
        if (oldestChangeRel != null) {
            Node oldestNode = oldestChangeRel.getEndNode();
            Node newestNode = getRoot().getSingleRelationship(GA_CHANGEFEED_NEXT_CHANGE, OUTGOING).getEndNode();
            if (newestNode != null) {
                long highSequence = (long) newestNode.getProperty(SEQUENCE);
                long lowSequence = (long) oldestNode.getProperty(SEQUENCE);
                long nodesToDelete = ((highSequence - lowSequence) + 1) - keep;
                Node newOldestNode = null;
                if (nodesToDelete > MAX_FEED_LENGTH_EXCEEDED) {
                    LOG.info("Preparing to prune change feed by deleting {} nodes", nodesToDelete);
                    getRoot().getSingleRelationship(GA_CHANGEFEED_OLDEST_CHANGE, OUTGOING).delete();
                    while (nodesToDelete > 0) {
                        Relationship rel = oldestNode.getSingleRelationship(GA_CHANGEFEED_NEXT_CHANGE, INCOMING);
                        newOldestNode = rel.getStartNode();
                        rel.delete();
                        oldestNode.delete();
                        oldestNode = newOldestNode;
                        nodesToDelete--;
                    }
                    getRoot().createRelationshipTo(newOldestNode, GA_CHANGEFEED_OLDEST_CHANGE);
                    LOG.info("_GA_ChangeFeed pruning complete");
                }
            }
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
            Relationship nextRel = getRoot().getSingleRelationship(GA_CHANGEFEED_NEXT_CHANGE, OUTGOING);
            if (nextRel != null) {
                startSequence = (Integer) nextRel.getEndNode().getProperty(SEQUENCE);
            }
            tx.success();
        }

        sequence = new AtomicInteger(startSequence);
    }
}
