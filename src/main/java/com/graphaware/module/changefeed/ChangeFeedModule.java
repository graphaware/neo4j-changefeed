package com.graphaware.module.changefeed;

import com.graphaware.runtime.BaseGraphAwareRuntimeModule;
import com.graphaware.tx.event.improved.api.ImprovedTransactionData;

import java.util.Deque;
import java.util.concurrent.ConcurrentLinkedDeque;

/**
 * {@link com.graphaware.runtime.GraphAwareRuntimeModule} that keeps track of changes in the graph
 */
public class ChangeFeedModule extends BaseGraphAwareRuntimeModule {

    private static final Deque<ChangeSet> changes = new ConcurrentLinkedDeque<>();
    private final int MAX_CHANGES = 3; //TODO this has to be configurable

    public ChangeFeedModule(String moduleId) {
        super(moduleId);
    }

    public static Deque<ChangeSet> getChanges() {
        return changes;
    }

    @Override
    public void beforeCommit(ImprovedTransactionData transactionData) {   //TODO this should be afterCommit
        if (transactionData.mutationsOccurred()) {
            ChangeSet changeset = new ChangeSet();
            changeset.setChanges(transactionData.mutationsToStrings());
            if (changes.size() == MAX_CHANGES) {
                changes.removeLast();
            }
            changes.addFirst(changeset);
        }
    }
}
