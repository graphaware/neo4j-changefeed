package com.graphaware.module.changefeed;

import com.graphaware.runtime.BaseGraphAwareRuntimeModule;
import com.graphaware.tx.event.improved.api.ImprovedTransactionData;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicInteger;

import static com.graphaware.common.util.IterableUtils.getSingleOrNull;
import static org.neo4j.tooling.GlobalGraphOperations.at;

/**
 * {@link com.graphaware.runtime.GraphAwareRuntimeModule} that keeps track of changes in the graph
 */
public class ChangeFeedModule extends BaseGraphAwareRuntimeModule {

    private ChangeFeed changeFeed;

    private  AtomicInteger sequence=new AtomicInteger(0);

    public ChangeFeedModule(String moduleId, GraphDatabaseService database) {
        super(moduleId);
        this.changeFeed=new ChangeFeed(database);
    }

    @Override
    public void initialize(GraphDatabaseService database) {
        try (Transaction tx=database.beginTx()) {
            Node result = getSingleOrNull(at(database).getAllNodesWithLabel(Labels.ChangeFeed));
            if (result == null) {
                database.createNode(Labels.ChangeFeed);
            }
            tx.success();
        }
        super.initialize(database);
    }

    @Override
    public void beforeCommit(ImprovedTransactionData transactionData) {   //TODO this should be afterCommit
        if (transactionData.mutationsOccurred()) {
            ChangeSet changeset = new ChangeSet();
            changeset.getChanges().addAll(transactionData.mutationsToStrings());
            changeset.setSequence(sequence.incrementAndGet());
            changeFeed.recordChange(changeset);
        }
    }
}
