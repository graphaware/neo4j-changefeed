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

import org.neo4j.graphdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static com.graphaware.common.util.IterableUtils.getSingleOrNull;
import static com.graphaware.module.changefeed.Properties.*;
import static org.neo4j.tooling.GlobalGraphOperations.at;

/**
 * {@link com.graphaware.module.changefeed.ChangeReader} that reads the changes stored in the graph.
 */
public class GraphChangeReader implements ChangeReader {

    private static final Logger LOG = LoggerFactory.getLogger(GraphChangeReader.class);

    private final GraphDatabaseService database;

    private Node root;

    /**
     * Construct a new repository.
     *
     * @param database in which to store the changes.
     */
    public GraphChangeReader(GraphDatabaseService database) {
        this.database = database;
    }

    @Override
    public Collection<ChangeSet> initialize(int limit) {
        return doGetChanges(null, limit);

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Collection<ChangeSet> getAllChanges() {
        return ChangeFeedFactory.getInstance().getChanges();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Collection<ChangeSet> getNumberOfChanges(int limit) {
        return ChangeFeedFactory.getInstance().getChanges(limit);
    }

    /**
     * {@inheritDoc}
     *
     * @param since
     */
    @Override
    public Collection<ChangeSet> getChangesSince(long since) {
        return ChangeFeedFactory.getInstance().getChangesSince(since);
        //return getNumberOfChangesSince(since, Integer.MAX_VALUE);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Collection<ChangeSet> getNumberOfChangesSince(long since, int limit) {
        return ChangeFeedFactory.getInstance().getChanges(since, limit);
        //return doGetChanges(since, limit);
    }

    /**
     * Get a list of changes from the graph.
     *
     * @param since the sequence number of the first change that will not be included in the result
     * @param limit number of changes to fetch
     * @return List of {@link com.graphaware.module.changefeed.ChangeSet}, latest change first.
     */
    private List<ChangeSet> doGetChanges(Long since, int limit) {
        int count = 0;
        List<ChangeSet> changeFeed = new ArrayList<>();
        Node start = getRoot();

        try (Transaction tx = database.beginTx()) {
            tx.acquireWriteLock(start); //We should not have to do this, temp workaround for https://github.com/neo4j/neo4j/issues/2677
            Relationship nextRel = start.getSingleRelationship(Relationships._GA_CHANGEFEED_NEXT_CHANGE, Direction.OUTGOING);

            while (count < limit && nextRel != null) {
                Node changeNode = nextRel.getEndNode();

                ChangeSet changeSet = new ChangeSet((long) changeNode.getProperty(SEQUENCE), (long) changeNode.getProperty(TIMESTAMP));
                if (since != null && changeSet.getSequence() <= since) {
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

    /**
     * Get the root.
     *
     * @return root, will never be null.
     */
    private Node getRoot() {
        if (root == null) {
            try (Transaction tx = database.beginTx()) {
                root = getSingleOrNull(at(database).getAllNodesWithLabel(Labels._GA_ChangeFeed));
                if (root == null) {
                    LOG.error("There is no ChangeFeed Root Node! Has the ChangeFeed Module been registered with the GraphAware Runtime? Has the Runtime been started?");
                    throw new IllegalStateException("There is no ChangeFeed Root Node! Has the ChangeFeed Module been registered with the GraphAware Runtime? Has the Runtime been started?");
                }
                tx.success();
            }
        }
        return root;
    }
}
