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

import com.graphaware.runtime.config.TxDrivenModuleConfiguration;
import com.graphaware.runtime.metadata.EmptyContext;
import com.graphaware.runtime.module.BaseTxDrivenModule;
import com.graphaware.runtime.module.TimerDrivenModule;
import com.graphaware.tx.event.improved.api.ImprovedTransactionData;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;

import static com.graphaware.common.util.IterableUtils.getSingleOrNull;
import static com.graphaware.module.changefeed.Relationships.GA_CHANGEFEED_NEXT_CHANGE;
import static com.graphaware.module.changefeed.Relationships.GA_CHANGEFEED_OLDEST_CHANGE;
import static org.neo4j.graphdb.Direction.INCOMING;
import static org.neo4j.graphdb.Direction.OUTGOING;
import static org.neo4j.tooling.GlobalGraphOperations.at;

/**
 * A {@link com.graphaware.runtime.module.TxDrivenModule} that keeps track of changes in the graph.
 * Also implements {@link TimerDrivenModule} to perform pruning of old changes.
 */
public class ChangeFeedModule extends BaseTxDrivenModule<Void> implements TimerDrivenModule<EmptyContext> {

    private static final Logger LOG = LoggerFactory.getLogger(ChangeFeedModule.class);

    private static final int PRUNE_DELAY = 5000;
    private static final String SEQUENCE_PROPERTY_KEY = "sequence";

    private final ChangeFeedConfiguration configuration;
    private final GraphChangeRepository repository;

    private AtomicInteger sequence = null;
    private GraphDatabaseService database;
    private Node root;

    public ChangeFeedModule(String moduleId, ChangeFeedConfiguration configuration, GraphDatabaseService database) {
        super(moduleId);
        this.configuration = configuration;
        this.database = database;
        this.repository = new GraphChangeRepository(database);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void start(GraphDatabaseService database) {
        root = getOrCreateRoot();
        initializeSequence();
    }

    /**
     * Get or create the root of the change feed.
     *
     * @return root.
     */
    private Node getOrCreateRoot() {
        Node root;

        try (Transaction tx = database.beginTx()) {
            root = getSingleOrNull(at(database).getAllNodesWithLabel(Labels._GA_ChangeFeed));
            if (root == null) {
                LOG.info("Creating the ChangeFeed Root");
                root = database.createNode(Labels._GA_ChangeFeed);
            }
            tx.success();
        }

        return root;
    }

    /**
     * Initialize the sequence to the last used number. No need to synchronize, called from constructor, thus in a single
     * thread.
     */
    private void initializeSequence() {
        int startSequence = 0;

        try (Transaction tx = database.beginTx()) {
            Relationship nextRel = getRoot().getSingleRelationship(GA_CHANGEFEED_NEXT_CHANGE, OUTGOING);
            if (nextRel != null) {
                startSequence = (Integer) nextRel.getEndNode().getProperty(SEQUENCE_PROPERTY_KEY);
            }
            tx.success();
        }

        sequence = new AtomicInteger(startSequence);
    }

    /**
     * Get the root.
     *
     * @return root, will never be null.
     */
    private Node getRoot() {
        if (root == null) {
            throw new IllegalStateException("There is not ChangeFeed Root! This is a bug. It looks like the start() method hasn't been called.");
        }
        return root;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TxDrivenModuleConfiguration getConfiguration() {
        return configuration;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Void beforeCommit(ImprovedTransactionData transactionData) {
        ChangeSet changeSet = new ChangeSet();
        changeSet.getChanges().addAll(transactionData.mutationsToStrings());
        changeSet.setSequence(sequence.incrementAndGet()); //TODO might this result in holes if a runtime exception is thrown at the end of this module or any other
        repository.recordChange(changeSet);

        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public EmptyContext createInitialContext(GraphDatabaseService database) {
        return new EmptyContext(System.currentTimeMillis() + PRUNE_DELAY);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public EmptyContext doSomeWork(EmptyContext lastContext, GraphDatabaseService database) {
        pruneChangeFeed(getRoot());
        return new EmptyContext(System.currentTimeMillis() + PRUNE_DELAY);
    }

    private void pruneChangeFeed(Node changeRoot) {
        final int MAX_FEED_LENGTH_EXCEEDED = 3;

        Relationship oldestChangeRel = changeRoot.getSingleRelationship(GA_CHANGEFEED_OLDEST_CHANGE, OUTGOING);
        if (oldestChangeRel != null) {
            Node oldestNode = oldestChangeRel.getEndNode();
            Node newestNode = changeRoot.getSingleRelationship(GA_CHANGEFEED_NEXT_CHANGE, OUTGOING).getEndNode();
            if (newestNode != null) {
                int highSequence = (int) newestNode.getProperty(SEQUENCE_PROPERTY_KEY);
                int lowSequence = (int) oldestNode.getProperty(SEQUENCE_PROPERTY_KEY);
                int nodesToDelete = ((highSequence - lowSequence) + 1) - configuration.getMaxChanges();
                Node newOldestNode = null;
                if (nodesToDelete > MAX_FEED_LENGTH_EXCEEDED) {
                    LOG.info("Preparing to prune change feed by deleting {} nodes", nodesToDelete);
                    changeRoot.getSingleRelationship(GA_CHANGEFEED_OLDEST_CHANGE, OUTGOING).delete();
                    while (nodesToDelete > 0) {
                        Relationship rel = oldestNode.getSingleRelationship(GA_CHANGEFEED_NEXT_CHANGE, INCOMING);
                        newOldestNode = rel.getStartNode();
                        rel.delete();
                        oldestNode.delete();
                        oldestNode = newOldestNode;
                        nodesToDelete--;
                    }
                    changeRoot.createRelationshipTo(newOldestNode, GA_CHANGEFEED_OLDEST_CHANGE);
                    LOG.info("_GA_ChangeFeed pruning complete");
                }
            }
        }
    }
}
