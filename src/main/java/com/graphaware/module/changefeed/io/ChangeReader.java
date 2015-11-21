/*
 * Copyright (c) 2013-2015 GraphAware
 *
 * This file is part of the GraphAware Framework.
 *
 * GraphAware Framework is free software: you can redistribute it and/or modify it under the terms of
 * the GNU General Public License as published by the Free Software Foundation, either
 * version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU General Public License for more details. You should have received a copy of
 * the GNU General Public License along with this program.  If not, see
 * <http://www.gnu.org/licenses/>.
 */

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
     * @param uuid uuid of the first change that will <b>not</b> be included in the result.
     * @return changes, latest one first.
     */
    Collection<ChangeSet> getChangesSince(String uuid);

    /**
     * Get latest changes since a certain point.
     *
     * @param uuid  uuid of the first change that will <b>not</b> be included in the result.
     * @param limit the maximum number of changes to return.
     * @return changes, latest one first. Note that if there are more changes since the given uuid than the limit,
     * the latest limit number of changes will be returned.
     */
    Collection<ChangeSet> getNumberOfChangesSince(String uuid, int limit);
}
