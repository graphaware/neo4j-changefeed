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
 * Keeps track of changes made to the graph
 */
public class ChangeFeed {

    private final int maxChanges;
    private final GraphDatabaseService database;
    private final ExecutionEngine executionEngine;
    private static final Logger LOG = LoggerFactory.getLogger(ChangeFeed.class);

    public ChangeFeed(GraphDatabaseService database) {
        this.maxChanges = ChangeFeedModule.getMaxChanges();
        LOG.info("Max changes is {}", maxChanges);
        this.database = database;
        executionEngine = new ExecutionEngine(database);
    }

    /**
     * Get a list of changes made to the graph, where each item represents all changes made within a transaction.
     *
     * @return List of {@link ChangeSet}, latest change first
     */
    public List<ChangeSet> getChanges() {
        return getChanges(null);
    }

    /**
     * Get a list of changes made to the graph since a known change
     *
     * @param since sequence number of a known change. All changes with sequence number greater than this parameter are returned.
     * @return List of {@link ChangeSet}, latest change first
     */
    public List<ChangeSet> getChanges(Integer since) {
        List<ChangeSet> changefeed = new ArrayList<>();
        ExecutionResult result;
        String getChangesQuery = "match (root:ChangeFeed)-[:NEXT*.." + maxChanges + "]->(change) return change";
        String getChangesSinceQuery = "match (startChange:ChangeSet {sequence: {sequence}}) with startChange match (startChange)<-[:NEXT*..]-(change:ChangeSet) return change order by change.sequence desc";
        try (Transaction tx = database.beginTx()) {
            if (since == null) {
                result = executionEngine.execute(getChangesQuery);
            } else {
                Map<String, Object> params = new HashMap<>();
                params.put("sequence", since);
                result = executionEngine.execute(getChangesSinceQuery, params);
            }
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

    public void recordChange(ChangeSet changeSet) {
        try (Transaction tx = database.beginTx()) {
            Node changeRoot = getSingleOrNull(at(database).getAllNodesWithLabel(Labels.ChangeFeed));
            if (changeRoot == null) {
                LOG.error("ChangeFeedModule not initialized!");
                throw new RuntimeException("Module not initialized"); //TODO throw the right exception
            }
            tx.acquireWriteLock(changeRoot);
            Node changeNode = database.createNode();
            changeNode.setProperty("sequence", changeSet.getSequence());
            changeNode.setProperty("changeDate", changeSet.getChangeDate().getTime());
            List<String> changeSetChanges = changeSet.getChanges();
            changeNode.setProperty("changes", changeSetChanges.toArray(new String[changeSetChanges.size()]));
            changeNode.addLabel(Labels.ChangeSet);

            Relationship firstChangeRel = changeRoot.getSingleRelationship(Relationships.NEXT, Direction.OUTGOING);
            if (firstChangeRel != null) {
                Node firstChange = firstChangeRel.getEndNode();
                tx.acquireWriteLock(firstChange);
                changeNode.createRelationshipTo(firstChange, Relationships.NEXT);
                firstChangeRel.delete();
            } else {
                changeRoot.createRelationshipTo(changeNode, Relationships.OLDEST_CHANGE);
            }
            changeRoot.createRelationshipTo(changeNode, Relationships.NEXT);

            //Now prune the feed, make it async
           /* int highSequence=changeSet.getSequence();
            Node oldestNode=changeRoot.getSingleRelationship(Relationships.OLDEST_CHANGE,Direction.OUTGOING).getEndNode();
            int lowSequence=(int)oldestNode.getProperty("sequence");
            int nodesToDelete=((highSequence-lowSequence) + 1) - maxChanges;
            Node newOldestNode=null;
            boolean deleteNodes=false;
            if(nodesToDelete>0) {
                changeRoot.getSingleRelationship(Relationships.OLDEST_CHANGE,Direction.OUTGOING).delete();
                deleteNodes=true;
            }
            while (nodesToDelete>0) {
                Relationship rel  = oldestNode.getSingleRelationship(Relationships.NEXT,Direction.INCOMING);
                newOldestNode=rel.getEndNode();
                rel.delete();
                oldestNode.delete();
                oldestNode=newOldestNode;
                nodesToDelete--;
            }
            if(deleteNodes) {
                changeRoot.createRelationshipTo(newOldestNode,Relationships.OLDEST_CHANGE);
            }*/
            tx.success();
        }

    }
}
