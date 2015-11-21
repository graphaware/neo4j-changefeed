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

package com.graphaware.module.changefeed.cache;

import com.graphaware.module.changefeed.domain.ChangeSet;
import com.graphaware.module.changefeed.io.GraphChangeWriter;
import org.neo4j.graphdb.GraphDatabaseService;

/**
 * {@link GraphChangeWriter} that also pushes all written changes to {@link ChangeSetCache}.
 */
public class CachingGraphChangeWriter extends GraphChangeWriter {

    private final ChangeSetCache cache;

    /**
     * Construct a new writer.
     *
     * @param database in which to store the changes.
     * @param moduleId ID of the module storing changes.
     * @param cache    for caching changes.
     */
    public CachingGraphChangeWriter(GraphDatabaseService database, String moduleId, ChangeSetCache cache) {
        super(database, moduleId);
        this.cache = cache;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void recordChanges(ChangeSet changeSet) {
        super.recordChanges(changeSet);
        cache.push(changeSet);
    }
}
