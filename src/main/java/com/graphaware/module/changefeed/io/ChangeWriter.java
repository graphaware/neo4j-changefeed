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
     * @param keep             number of changes to keep.
     * @param mustBeExceededBy number of changes in the database by which the <code>keep</code> parameter
     *                         must be exceeded before the oldest changes are actually pruned.
     */
    void pruneChanges(int keep, int mustBeExceededBy);
}
