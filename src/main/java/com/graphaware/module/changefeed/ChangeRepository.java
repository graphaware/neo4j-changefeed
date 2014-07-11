package com.graphaware.module.changefeed;

import java.util.List;
import java.util.Set;

/**
 * A repository of {@link ChangeSet}s.
 */
public interface ChangeRepository {

    /**
     * Initialize the repository before it can be used.
     */
    void initialize();

    /**
     * Get all changes.
     *
     * @return all changes, last one first.
     */
    List<ChangeSet> getAllChanges();

    /**
     * Get latest changes.
     *
     * @param limit the maximum number of changes to return.
     * @return changes, latest one first.
     */
    List<ChangeSet> getNumberOfChanges(int limit);

    /**
     * Get all changes since a certain point.
     *
     * @param since sequence number of the first change that will <b>not</b> be included in the result.
     * @return changes, latest one first.
     */
    List<ChangeSet> getChangesSince(int since);

    /**
     * Get latest changes since a certain point.
     *
     * @param since sequence number of the first change that will <b>not</b> be included in the result.
     * @param limit the maximum number of changes to return.
     * @return changes, latest one first. Note that if there are more changes since the given sequence number than the limit,
     *         the latest limit number of changes will be returned.
     */
    List<ChangeSet> getNumberOfChangesSince(int since, int limit);

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
