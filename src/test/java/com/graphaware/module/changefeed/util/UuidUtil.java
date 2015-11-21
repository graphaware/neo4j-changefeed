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

package com.graphaware.module.changefeed.util;

import com.graphaware.module.changefeed.domain.Labels;
import com.graphaware.module.changefeed.domain.Relationships;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

import static com.graphaware.common.util.IterableUtils.getSingleOrNull;
import static com.graphaware.module.changefeed.domain.Properties.MODULE_ID;
import static com.graphaware.module.changefeed.domain.Properties.UUID;

public class UuidUtil {

    public static String getUuidOfLatestChange(GraphDatabaseService database) {
        String uuid;

        try (Transaction tx = database.beginTx()) {
            Node root = getSingleOrNull(database.findNodesByLabelAndProperty(Labels._GA_ChangeFeed, MODULE_ID, "CFM"));
            if (root == null) {
                throw new IllegalStateException("The ChangeFeed node should have been created");
            }
            Node latestChangeNode = root.getSingleRelationship(Relationships._GA_CHANGEFEED_NEXT_CHANGE, Direction.OUTGOING).getEndNode();
            uuid = (String) latestChangeNode.getProperty(UUID);
            tx.success();
        }
        return uuid;
    }
}
