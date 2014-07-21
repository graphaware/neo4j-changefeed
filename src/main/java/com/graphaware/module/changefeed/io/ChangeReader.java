package com.graphaware.module.changefeed.io;

import com.graphaware.module.changefeed.domain.ChangeSet;

import java.util.Collection;

/**
 * A reader of {@link com.graphaware.module.changefeed.domain.ChangeSet}s.
 */
public interface ChangeReader {

    /**
     * Get all changes.
     *
     * @return all changes, last one first.
     */
    Collection<ChangeSet> getAllChanges();

    /**
     * Get latest changes.
     *
     * @param limit the maximum number of changes to return.
     * @return changes, latest one first.
     */
    Collection<ChangeSet> getNumberOfChanges(int limit);

    /**
     * Get all changes since a certain point.
     *
     * @param since sequence number of the first change that will <b>not</b> be included in the result.
     * @return changes, latest one first.
     */
    Collection<ChangeSet> getChangesSince(long since);

    /**
     * Get latest changes since a certain point.
     *
     * @param since sequence number of the first change that will <b>not</b> be included in the result.
     * @param limit the maximum number of changes to return.
     * @return changes, latest one first. Note that if there are more changes since the given sequence number than the limit,
     * the latest limit number of changes will be returned.
     */
    Collection<ChangeSet> getNumberOfChangesSince(long since, int limit);
}
