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

import org.neo4j.graphdb.GraphDatabaseService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.List;

/**
 * REST API for {@link ChangeFeed}.
 */
@Controller
@RequestMapping("/changefeed")
public class ChangeFeedApi {

    private final ChangeFeed changeFeed;

    @Autowired
    public ChangeFeedApi(GraphDatabaseService database) {
        changeFeed = new ChangeFeed(database);

    }


    /**
     * Get a list of changes made to the graph, where each item represents all changes made within a transaction.
     *
     * @param since sequence number (optional). All changes with sequence number greater than this parameter are returned.
     * @return List of {@link com.graphaware.module.changefeed.ChangeSet}, latest change first
     */
    @RequestMapping(method = RequestMethod.GET)
    @ResponseBody
    public List<ChangeSet> getChangeFeed(@RequestParam(value = "since", required = false) Integer since) {
        return changeFeed.getChanges(since);
    }
}
