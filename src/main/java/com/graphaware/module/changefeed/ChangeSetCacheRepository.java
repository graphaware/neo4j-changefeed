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

package com.graphaware.module.changefeed;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ChangeSetCacheRepository {

    private final Map<String, ChangeSetCache> caches = new ConcurrentHashMap<>();

    public void registerCache(String moduleId, ChangeSetCache cache) {
        caches.put(moduleId, cache);
    }

    public ChangeSetCache getCache(String moduleId) {
        if (!caches.containsKey(moduleId)) {
            throw new IllegalStateException("There is no change set cache for module ID " + moduleId + "." +
                    " Please check that the module has been registered with GraphAware Runtime and that the Runtime has been started.");
        }

        return caches.get(moduleId);
    }
}
