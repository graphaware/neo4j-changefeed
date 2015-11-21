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

import com.graphaware.module.changefeed.ChangeFeedModule;
import com.graphaware.module.changefeed.domain.ChangeSet;
import com.graphaware.module.changefeed.io.GraphChangeReader;
import org.neo4j.graphdb.GraphDatabaseService;

import java.util.Collection;

import static com.graphaware.runtime.RuntimeRegistry.getStartedRuntime;

/**
 * {@link GraphChangeReader} which reads the changes from {@link ChangeSetCache}.
 */
public class CachingGraphChangeReader extends GraphChangeReader {

    private final ChangeSetCache cache;

    /**
     * Construct a new reader.
     * <p/>
     * Use this API if a single {@link ChangeFeedModule} is registered with module ID equal to {@link ChangeFeedModule#DEFAULT_MODULE_ID}.
     *
     * @param database in which the changes are stored.
     */
    public CachingGraphChangeReader(GraphDatabaseService database) {
        this(database, ChangeFeedModule.DEFAULT_MODULE_ID);
    }

    /**
     * Construct a new reader.
     *
     * @param database in which the changes are stored.
     * @param moduleId ID of the module storing changes.
     */
    public CachingGraphChangeReader(GraphDatabaseService database, String moduleId) {
        super(database, moduleId);

        cache = getStartedRuntime(database).getModule(moduleId, ChangeFeedModule.class).getChangesCache();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Collection<ChangeSet> doGetChanges(String uuid, int limit) {
        return cache.getChanges(uuid, limit);
    }
}
