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
 * REST API for {@link ChangeFeedModule}.
 */
@Controller
@RequestMapping("/changefeed")
public class ChangeFeedApi {

    private final ChangeRepository changeRepository;

    @Autowired
    public ChangeFeedApi(GraphDatabaseService database) {
        changeRepository = new GraphChangeRepository(database);

    }

    /**
     * Get a list of changes made to the graph, where each item represents all changes made within a transaction.
     *
     * @param since sequence number (optional). All changes with sequence number greater than this parameter are returned.
     * @param limit maximum number of changes to return (optional). Note that this is upper limit only, there might not be that many changes.
     * @return List of {@link com.graphaware.module.changefeed.ChangeSet}, latest change first.
     */
    @RequestMapping(method = RequestMethod.GET)
    @ResponseBody
    public List<ChangeSet> getChangeFeed(@RequestParam(value = "since", required = false) Integer since, @RequestParam(value = "limit", required = false) Integer limit) {
        if (since == null && limit == null) {
            return changeRepository.getAllChanges();
        }

        if (since == null) {
            return changeRepository.getNumberOfChanges(limit);
        }

        if (limit == null) {
            return changeRepository.getChangesSince(since);
        }

        return changeRepository.getNumberOfChangesSince(since, limit);
    }
}
