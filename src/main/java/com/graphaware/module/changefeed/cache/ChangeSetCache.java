/*
 * Copyright (c) 2014 GraphAware
 *
 * This file is part of GraphAware.
 *
 * GraphAware is free software: you can redistribute it and/or modify it under the terms of
 * the GNU General Public License as published by the Free Software Foundation, either
 * version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 *  without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU General Public License for more details. You should have received a copy of
 * the GNU General Public License along with this program.  If not, see
 * <http://www.gnu.org/licenses/>.
 */

package com.graphaware.module.changefeed.cache;

import com.graphaware.module.changefeed.domain.ChangeSet;
import com.graphaware.module.changefeed.util.BoundedConcurrentStack;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

/**
 * {@link com.graphaware.module.changefeed.util.BoundedConcurrentStack} of {@link com.graphaware.module.changefeed.domain.ChangeSet}s,
 * intended to be used as a cache of configurable number of latest {@link com.graphaware.module.changefeed.domain.ChangeSet}s.
 */
public class ChangeSetCache {

    private final BoundedConcurrentStack<ChangeSet> changes;

    /**
     * Construct a new cache with given capacity.
     *
     * @param capacity of the cache.
     */
    public ChangeSetCache(int capacity) {
        changes = new BoundedConcurrentStack<>(capacity);
    }

    /**
     * Push a change set into the cache.
     *
     * @param changeSet to push.
     */
    public void push(ChangeSet changeSet) {
        changes.push(changeSet);
    }

    /**
     * Populate the cache with change sets.
     *
     * @param changeSets to populate the cache with. These are expected to be ordered from newest to oldest.
     */
    public void populate(Collection<ChangeSet> changeSets) {
        changes.populate(changeSets);
    }

    /**
     * Get a number of latest changes newer than the given sequence number.
     *
     * @param sequence sequence number of all returned changes will be greater than this number.
     * @param limit    the number of changes to get.
     * @return changes ordered from newest to oldest. The maximum number of changes returned is limited by the capacity
     *         of the cache, the number of changes currently in cache, the provided limit, and the provided sequence,
     *         whichever returns fewer changes.
     */
    public Collection<ChangeSet> getChanges(long sequence, int limit) {
        List<ChangeSet> result = new LinkedList<>();

        for (ChangeSet changeSet : changes) {
            if (changeSet.getSequence() > sequence) {
                result.add(changeSet);
            }
            if (result.size() >= limit) {
                return result;
            }
        }

        return result;
    }
}
