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

package com.graphaware.module.changefeed.api;

import com.graphaware.module.changefeed.cache.CachingGraphChangeReader;
import com.graphaware.module.changefeed.domain.ChangeSet;
import com.graphaware.module.changefeed.io.ChangeReader;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.NotFoundException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import java.util.Collection;

import static com.graphaware.module.changefeed.ChangeFeedModule.DEFAULT_MODULE_ID;

/**
 * REST API for {@link com.graphaware.module.changefeed.ChangeFeedModule}.
 */
@Controller
@RequestMapping("/changefeed")
public class ChangeFeedApi {

    private final GraphDatabaseService database;

    @Autowired
    public ChangeFeedApi(GraphDatabaseService database) {
        this.database = database;
    }

    /**
     * Get a list of changes made to the graph, where each item represents all changes made within a transaction.
     * Use this API if a single {@link com.graphaware.module.changefeed.ChangeFeedModule} is registered with module ID equal to {@link com.graphaware.module.changefeed.ChangeFeedModule#DEFAULT_MODULE_ID}.
     *
     * @param uuid  uuid of change set (optional). All changes which occur after the change with this uuid will be returned
     * @param limit maximum number of changes to return (optional). Note that this is upper limit only, there might not be that many changes.
     * @return Collection of {@link com.graphaware.module.changefeed.domain.ChangeSet}, latest change first.
     */
    @RequestMapping(value = "/", method = RequestMethod.GET)
    @ResponseBody
    public Collection<ChangeSet> getChangeFeed(@RequestParam(value = "uuid", required = false) String uuid, @RequestParam(value = "limit", required = false) Integer limit) {
        return getChangeFeed(DEFAULT_MODULE_ID, uuid, limit);
    }

    /**
     * Get a list of changes made to the graph, where each item represents all changes made within a transaction.
     *
     * @param moduleId ID of the {@link com.graphaware.module.changefeed.ChangeFeedModule} that has written the changes.
     * @param uuid      uuid of change set (optional). All changes which occur after the change with this uuid will be returned
     * @param limit    maximum number of changes to return (optional). Note that this is upper limit only, there might not be that many changes.
     * @return Collection of {@link com.graphaware.module.changefeed.domain.ChangeSet}, latest change first.
     */
    @RequestMapping(value = "/{moduleId}", method = RequestMethod.GET)
    @ResponseBody
    public Collection<ChangeSet> getChangeFeed(@PathVariable String moduleId, @RequestParam(value = "uuid", required = false) String uuid, @RequestParam(value = "limit", required = false) Integer limit) {
        ChangeReader changeReader = new CachingGraphChangeReader(database, moduleId);

        if (uuid == null && limit == null) {
            return changeReader.getAllChanges();
        }

        if (uuid == null) {
            return changeReader.getNumberOfChanges(limit);
        }

        if (limit == null) {
            return changeReader.getChangesSince(uuid);
        }

        return changeReader.getNumberOfChangesSince(uuid, limit);
    }


    @ExceptionHandler(IllegalArgumentException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public void handleIllegalArguments() {
    }

    @ExceptionHandler(NotFoundException.class)
    @ResponseStatus(HttpStatus.NOT_FOUND)
    public void handleNotFound() {
    }
}
