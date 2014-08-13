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

package com.graphaware.module.changefeed.io;

import com.graphaware.module.changefeed.ChangeFeedModule;
import com.graphaware.module.changefeed.domain.ChangeSet;
import com.graphaware.module.changefeed.domain.Labels;
import com.graphaware.module.changefeed.domain.Relationships;
import org.neo4j.graphdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static com.graphaware.common.util.IterableUtils.getSingleOrNull;
import static com.graphaware.module.changefeed.domain.Properties.*;

/**
 * {@link ChangeReader} that reads the changes stored in the graph.
 */
public class GraphChangeReader implements ChangeReader {

    private static final Logger LOG = LoggerFactory.getLogger(GraphChangeReader.class);

    private final GraphDatabaseService database;
    private final Node root;

    /**
     * Construct a new reader.
     * <p/>
     * Use this API if a single {@link com.graphaware.module.changefeed.ChangeFeedModule} is registered with module ID equal to {@link com.graphaware.module.changefeed.ChangeFeedModule#DEFAULT_MODULE_ID}.
     *
     * @param database in which the changes are stored.
     */
    public GraphChangeReader(GraphDatabaseService database) {
        this(database, ChangeFeedModule.DEFAULT_MODULE_ID);
    }

    /**
     * Construct a new reader.
     *
     * @param database in which the changes are stored.
     * @param moduleId ID of the module storing changes.
     */
    public GraphChangeReader(GraphDatabaseService database, String moduleId) {
        this.database = database;

        try (Transaction tx = database.beginTx()) {
            root = getSingleOrNull(database.findNodesByLabelAndProperty(Labels._GA_ChangeFeed, MODULE_ID, moduleId));
            if (root == null) {
                LOG.error("There is no ChangeFeed Root Node for module ID " + moduleId + "! Has the ChangeFeed Module been registered with the GraphAware Runtime? Has the Runtime been started?");
                throw new NotFoundException("There is no ChangeFeed Root Node for module ID " + moduleId + "! Has the ChangeFeed Module been registered with the GraphAware Runtime? Has the Runtime been started?");
            }
            tx.success();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Collection<ChangeSet> getAllChanges() {
        return getChangesSince(null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Collection<ChangeSet> getNumberOfChanges(int limit) {
        return getNumberOfChangesSince(null, limit);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Collection<ChangeSet> getChangesSince(String uuid) {
        return getNumberOfChangesSince(uuid, Integer.MAX_VALUE);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Collection<ChangeSet> getNumberOfChangesSince(String uuid, int limit) {
        return doGetChanges(uuid, limit);
    }

    /**
     * Get a list of changes from the graph.
     *
     * @param uuid  the uuid of the first change that will not be included in the result, use null for all.
     * @param limit number of changes to fetch
     * @return List of {@link com.graphaware.module.changefeed.domain.ChangeSet}, latest change first.
     */
    protected Collection<ChangeSet> doGetChanges(String uuid, int limit) {
        int count = 0;
        List<ChangeSet> changeFeed = new ArrayList<>();

        try (Transaction tx = database.beginTx()) {
            tx.acquireWriteLock(root); //We should not have to do this, temp workaround for https://github.com/neo4j/neo4j/issues/2677
            Relationship nextRel = root.getSingleRelationship(Relationships._GA_CHANGEFEED_NEXT_CHANGE, Direction.OUTGOING);

            while (count < limit && nextRel != null) {
                Node changeNode = nextRel.getEndNode();

                ChangeSet changeSet = new ChangeSet((String) changeNode.getProperty(UUID), (long) changeNode.getProperty(TIMESTAMP));
                if (uuid != null && changeSet.getUuid().equals(uuid)) {
                    break;
                }
                changeSet.addChanges((String[]) changeNode.getProperty(CHANGES));
                changeFeed.add(changeSet);
                count++;

                nextRel = changeNode.getSingleRelationship(Relationships._GA_CHANGEFEED_NEXT_CHANGE, Direction.OUTGOING);
            }
            tx.success();
        }

        return changeFeed;
    }
}
