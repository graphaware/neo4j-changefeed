package com.graphaware.module.changefeed;

import com.graphaware.runtime.BaseGraphAwareRuntimeModule;
import com.graphaware.tx.event.improved.api.ImprovedTransactionData;
import org.neo4j.graphdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static com.graphaware.common.util.IterableUtils.getSingleOrNull;
import static org.neo4j.tooling.GlobalGraphOperations.at;

/**
 * {@link com.graphaware.runtime.GraphAwareRuntimeModule} that keeps track of changes in the graph
 */
public class ChangeFeedModule extends BaseGraphAwareRuntimeModule {

    private static final int MAX_CHANGES_DEFAULT = 50;
    private static int maxChanges = MAX_CHANGES_DEFAULT;
    private static final Logger LOG = LoggerFactory.getLogger(ChangeFeedModule.class);


    private final ChangeFeed changeFeed;
    private AtomicInteger sequence;

    public ChangeFeedModule(String moduleId, GraphDatabaseService database, Map<String, String> config) {
        super(moduleId);
        if (config.get("maxChanges") != null) {
            maxChanges = Integer.parseInt(config.get("maxChanges"));
            LOG.info("MaxChanges set to {}", maxChanges);
        }
        sequence=new AtomicInteger(0);
        /*int startSequence = 0;   //TODO put this in the right place
        try (Transaction tx = database.beginTx()) {
            Node result = getSingleOrNull(at(database).getAllNodesWithLabel(Labels.ChangeFeed));
            if (result != null) {
                Relationship nextRel = result.getSingleRelationship(Relationships.NEXT, Direction.OUTGOING);
                if (nextRel != null) {
                    startSequence = (Integer) nextRel.getEndNode().getProperty("sequence");
                }
            }
            sequence = new AtomicInteger(startSequence);
            tx.success();
        }*/
        this.changeFeed = new ChangeFeed(database);
    }

    public static int getMaxChanges() {
        return maxChanges;
    }

    @Override
    public void initialize(GraphDatabaseService database) {
        int startSequence = 0;
        try (Transaction tx = database.beginTx()) {
            Node result = getSingleOrNull(at(database).getAllNodesWithLabel(Labels.ChangeFeed));
            if (result == null) {
                LOG.info("Creating the ChangeFeed root");
                database.createNode(Labels.ChangeFeed);
            } else {    //TODO doesn't seem to be the right place to put this. Figure it out asap. Till then, the sequence will be screwed up on restart of neo4j
                Relationship nextRel = result.getSingleRelationship(Relationships.NEXT, Direction.OUTGOING);
                if (nextRel != null) {
                    startSequence = (Integer) nextRel.getEndNode().getProperty("sequence");
                }
            }
            sequence = new AtomicInteger(startSequence);
            tx.success();
        }
        LOG.info("Initialized ChangeFeedModule");
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
