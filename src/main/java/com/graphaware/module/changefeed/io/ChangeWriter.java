package com.graphaware.module.changefeed.io;

import java.util.Set;

/**
 * A writer of {@link com.graphaware.module.changefeed.domain.ChangeSet}s.
 */
public interface ChangeWriter {

    /**
     * Initialize the writer before it can be used.
     */
    void initialize();

    /**
     * Record (persist) a set of changes.
     *
     * @param changes to record.
     */
    void recordChanges(Set<String> changes);

    /**
     * Prune the changes, only keeping the specified number of latest changes.
     *
     * @param keep number of changes to keep.
     */
    void pruneChanges(int keep);
}
