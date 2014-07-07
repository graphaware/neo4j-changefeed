package com.graphaware.module.changefeed;

import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.graphdb.*;
import org.neo4j.helpers.collection.MapUtil;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedDeque;

import static com.graphaware.common.util.IterableUtils.getSingleOrNull;
import static org.neo4j.tooling.GlobalGraphOperations.at;

/**
 * Created by luanne on 04/07/14.
 */
public class ChangeFeed {

    private final static int MAX_CHANGES = 3; //TODO this has to be configurable
    private final Deque<ChangeSet> changes = new ConcurrentLinkedDeque<>();
    private final GraphDatabaseService database;
    private final ExecutionEngine executionEngine;
    private final String createChangeSetQuery=
            "CREATE (change:ChangeSet {sequence: {sequence}, changeDate: {changeDate}, changes: {changes}}) with change " +
            "MATCH (changeRoot:ChangeFeed) " +
            "OPTIONAL MATCH (changeRoot)-[oldLink:NEXT]->(oldHead) " +
            "MERGE (changeRoot)-[:NEXT]->(change) " +
            "WITH change,oldLink,oldHead " +
            "WHERE not(oldLink is null) " +
            "MERGE (change)-[:NEXT]->(oldHead) " +
            "DELETE oldLink";

    private final String getChangesQuery="match (root:ChangeFeed)-[:NEXT*.." + MAX_CHANGES + "]->(change) return change";

    public ChangeFeed(GraphDatabaseService database) {
        this.database = database;
        executionEngine = new ExecutionEngine(database);
    }

    public List<ChangeSet> getChanges() {
        List<ChangeSet> changefeed = new ArrayList<>();
        try (Transaction tx = database.beginTx()) {
            ExecutionResult result = executionEngine.execute(getChangesQuery);
            Iterator<Node> resultsIt = result.columnAs("change");
            while (resultsIt.hasNext()) {
                Node changeNode = resultsIt.next();
                ChangeSet changeSet = new ChangeSet();
                changeSet.setSequence((Integer) changeNode.getProperty("sequence"));
                changeSet.setChangeDate(new Date((Long) changeNode.getProperty("changeDate")));
                changeSet.setChanges(Arrays.asList((String[]) changeNode.getProperty("changes")));
                changefeed.add(changeSet);
            }
        }
        return changefeed;
    }


    public void recordChange(ChangeSet changeSet) {
        try (Transaction tx=database.beginTx()) {
            Node changeRoot = getSingleOrNull(at(database).getAllNodesWithLabel(Labels.ChangeFeed));
            if (changeRoot == null) {
                throw new RuntimeException("Module not initialized"); //TODO throw the right exception
            }
            tx.acquireWriteLock(changeRoot);
            Node changeNode=database.createNode();
            changeNode.setProperty("sequence",changeSet.getSequence());
            changeNode.setProperty("changeDate",changeSet.getChangeDate().getTime());
            List<String> changeSetChanges = changeSet.getChanges();
            changeNode.setProperty("changes", changeSetChanges.toArray(new String[changeSetChanges.size()]));
            changeNode.addLabel(Labels.ChangeSet);

            Relationship firstChangeRel = changeRoot.getSingleRelationship(Relationships.NEXT, Direction.OUTGOING);
            if(firstChangeRel!=null) {
                Node firstChange=firstChangeRel.getEndNode();
                tx.acquireWriteLock(firstChange);
                changeNode.createRelationshipTo(firstChange,Relationships.NEXT);
                firstChangeRel.delete();
            }
            changeRoot.createRelationshipTo(changeNode,Relationships.NEXT);
            tx.success();
        }

        /*Map<String, Object> params = new HashMap<>();
        params.put("sequence", changeSet.getSequence());
        params.put("changeDate", changeSet.getChangeDate().getTime());
        params.put("changes", changeSet.getChanges().toArray(new String[0]));
        executionEngine.execute(createChangeSetQuery, params);*/
}
}
