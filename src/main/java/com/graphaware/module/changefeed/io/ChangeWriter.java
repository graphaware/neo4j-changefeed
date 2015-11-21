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
