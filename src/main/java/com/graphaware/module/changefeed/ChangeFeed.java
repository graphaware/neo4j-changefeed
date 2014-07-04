package com.graphaware.module.changefeed;

import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.graphdb.GraphDatabaseService;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedDeque;

/**
 * Created by luanne on 04/07/14.
 */
public class ChangeFeed {

    private final static int MAX_CHANGES = 3; //TODO this has to be configurable
    private final Deque<ChangeSet> changes = new ConcurrentLinkedDeque<>();
    private final GraphDatabaseService database;
    private final ExecutionEngine executionEngine;
    private final String cypher=
            "CREATE (change:ChangeSet {sequence: {sequence}, changeDate: {changeDate}, changes: {changes}}) with change " +
            "MATCH (changeRoot:ChangeFeed) " +
            "OPTIONAL MATCH (changeRoot)-[oldLink:NEXT]->(oldHead) " +
            "MERGE (changeRoot)-[:NEXT]->(change) " +
            "WITH change,oldLink,oldHead " +
            "WHERE not(oldLink is null) " +
            "MERGE (change)-[:NEXT]->(oldHead) " +
            "DELETE oldLink";

    public ChangeFeed(GraphDatabaseService database) {
        this.database = database;
        executionEngine = new ExecutionEngine(database);
    }

    public List<ChangeSet> getChanges() {
        List<ChangeSet> changefeed = new ArrayList<>();
        Iterator<ChangeSet> it = changes.iterator();
        int count = 0;
        while (it.hasNext() && count++ < MAX_CHANGES) {
            changefeed.add(it.next());
        }

        return changefeed;
    }


    public void recordChange(ChangeSet changeSet) {
        Map<String, Object> params = new HashMap<>();
        params.put("sequence", changeSet.getSequence());
        params.put("changeDate", changeSet.getChangeDate().getTime());
        params.put("changes", changeSet.getChanges());
        executionEngine.execute(cypher, params);
}
}
