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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * An in-memory repository of {@link ChangeSetCache}s keyed by ID of the {@link com.graphaware.module.changefeed.ChangeFeedModule}.
 */
public class ChangeSetCacheRepository {

    private final Map<String, ChangeSetCache> caches = new ConcurrentHashMap<>();

    /**
     * Register a cache for module ID.
     *
     * @param moduleId module ID.
     * @param cache    to register.
     */
    public void registerCache(String moduleId, ChangeSetCache cache) {
        caches.put(moduleId, cache);
    }

    /**
     * Get a cache for a module ID.
     *
     * @param moduleId module ID.
     * @return the cache.
     * @throws IllegalStateException in case there is no cache registered for the module ID.
     */
    public ChangeSetCache getCache(String moduleId) {
        if (!caches.containsKey(moduleId)) {
            throw new IllegalStateException("There is no change set cache for module ID " + moduleId + "." +
                    " Please check that the module has been registered with GraphAware Runtime and that the Runtime has been started.");
        }

        return caches.get(moduleId);
    }

    /**
     * Clear all caches.
     */
    public void clear() {
        caches.clear();
    }
}
