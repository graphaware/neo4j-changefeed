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

package com.graphaware.module.changefeed.domain;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.graphaware.common.util.ArrayUtils;

import java.util.*;

/**
 * A set of changes applied on the graph in a single transaction.
 * <p/>
 * Note that the changes are organised in a list for predictable order (creates, then deletes, etc.), but the ordering
 * within a particular change set does not resemble the real ordering of the operations in the transaction.
 */
public class ChangeSet {

    private final String uuid;
    private final long timestamp;
    private final List<String> changes = new LinkedList<>();

    /**
     * Construct a new change set with timestamp of now.
     *
     * @param uuid uuid identifying the change set.
     */
    public ChangeSet(String uuid) {
        this(uuid, new Date().getTime());
    }

    /**
     * Construct a new change set.
     *
     * @param uuid      uuid identifying the change set.
     * @param timestamp of the change set.
     */
    public ChangeSet(String uuid, long timestamp) {
        this.uuid = uuid;
        this.timestamp = timestamp;
    }


    /**
     * Get the timestamp of this change set.
     *
     * @return time when the transaction performing these changes started committing in ms since 1/1/1970.
     */
    public long getTimestamp() {
        return timestamp;
    }

    /**
     * Get the uuid of this change set.
     *
     * @return uuid identifying this change set
     */
    public String getUuid() {
        return uuid;
    }

    /**
     * Add changes to this change set.
     *
     * @param changes to add.
     */
    public void addChanges(Collection<String> changes) {
        this.changes.addAll(changes);
    }

    /**
     * Add changes to this change set.
     *
     * @param changes to add.
     */
    public void addChanges(String... changes) {
        this.changes.addAll(Arrays.asList(changes));
    }

    /**
     * Get all the changes in this change set.
     *
     * @return all changes in a read-only list.
     */
    public List<String> getChanges() {
        return Collections.unmodifiableList(changes);
    }

    /**
     * Get all the changes in this change set.
     *
     * @return all changes in an array.
     */
    @JsonIgnore
    public String[] getChangesAsArray() {
        return changes.toArray(new String[changes.size()]);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return "ChangeSet{" +
                "uuid='" + uuid + '\'' +
                ", timestamp=" + timestamp +
                ", changes=" + ArrayUtils.primitiveOrStringArrayToString(changes) +
                '}';
    }
}
