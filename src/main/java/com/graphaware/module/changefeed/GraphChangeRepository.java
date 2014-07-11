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

import static com.graphaware.common.util.IterableUtils.getSingleOrNull;
import static org.neo4j.tooling.GlobalGraphOperations.at;

/**
 * {@link ChangeRepository} that keeps the changes stored in the graph.
 */
public class GraphChangeRepository implements ChangeRepository {

    private final GraphDatabaseService database;
    private final ExecutionEngine executionEngine;
    private static final Logger LOG = LoggerFactory.getLogger(GraphChangeRepository.class);

    private static final String CHANGES_SINCE_QUERY = "match (startChange:_GA_ChangeSet {sequence: {sequence}}) with startChange match (startChange)<-[:GA_CHANGEFEED_NEXT_CHANGE*..]-(change:_GA_ChangeSet) return change order by change.sequence desc";

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
    public List<ChangeSet> getAllChanges() {
        return getChanges("match (root:_GA_ChangeFeed)-[:GA_CHANGEFEED_NEXT_CHANGE*.." + Integer.MAX_VALUE + "]->(change) return change",  Collections.<String, Object>emptyMap());
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
        params.put("sequence", since);
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
         }
         catch (Exception e) {
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
            Iterator<Node> resultsIt = result.columnAs("change");
            while (resultsIt.hasNext()) {
                Node changeNode = resultsIt.next();
                ChangeSet changeSet = new ChangeSet();
                changeSet.setSequence((Integer) changeNode.getProperty("sequence"));
                changeSet.setChangeDate(new Date((Long) changeNode.getProperty("changeDate")));
                changeSet.setChanges(Arrays.asList((String[]) changeNode.getProperty("changes")));
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
    public void recordChange(ChangeSet changeSet) {
        try (Transaction tx = database.beginTx()) {
            Node changeRoot = getSingleOrNull(at(database).getAllNodesWithLabel(Labels._GA_ChangeFeed));
            if (changeRoot == null) {
                LOG.error("ChangeFeedModule not initialized!");
                throw new IllegalStateException("Module not initialized");
            }
            tx.acquireWriteLock(changeRoot);
            Node changeNode = database.createNode();
            changeNode.setProperty("sequence", changeSet.getSequence());
            changeNode.setProperty("changeDate", changeSet.getChangeDate().getTime());
            List<String> changeSetChanges = changeSet.getChanges();
            changeNode.setProperty("changes", changeSetChanges.toArray(new String[changeSetChanges.size()]));
            changeNode.addLabel(Labels._GA_ChangeSet);

            Relationship firstChangeRel = changeRoot.getSingleRelationship(Relationships.GA_CHANGEFEED_NEXT_CHANGE, Direction.OUTGOING);
            if (firstChangeRel == null) { //First changeSet recorded, create an GA_CHANGEFEED_OLDEST_CHANGE relation from the root to it
                changeRoot.createRelationshipTo(changeNode, Relationships.GA_CHANGEFEED_OLDEST_CHANGE);
            } else {
                Node firstChange = firstChangeRel.getEndNode();
                tx.acquireWriteLock(firstChange);
                changeNode.createRelationshipTo(firstChange, Relationships.GA_CHANGEFEED_NEXT_CHANGE);
                firstChangeRel.delete();
            }

            changeRoot.createRelationshipTo(changeNode, Relationships.GA_CHANGEFEED_NEXT_CHANGE);

            tx.success();
        }

    }
}
