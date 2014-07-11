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
import com.graphaware.runtime.metadata.NodeBasedContext;
import com.graphaware.runtime.module.BaseTxDrivenModule;
import com.graphaware.runtime.module.TimerDrivenModule;
import com.graphaware.tx.event.improved.api.ImprovedTransactionData;
import org.neo4j.graphdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;

import static com.graphaware.common.util.IterableUtils.getSingleOrNull;
import static org.neo4j.tooling.GlobalGraphOperations.at;

/**
 * A GraphAware {@link com.graphaware.runtime.module.TxDrivenModule} that keeps track of changes in the graph.
 * Also implements {@link TimerDrivenModule} to perform pruning of old changes.
 */
public class ChangeFeedModule extends BaseTxDrivenModule<Void> implements TimerDrivenModule<NodeBasedContext> {

    private static final Logger LOG = LoggerFactory.getLogger(ChangeFeedModule.class);
    private static final Object mutex = new Object();
    public static final int PRUNE_DELAY = 5000;
    private final ChangeFeedConfiguration configuration;

    private final ChangeFeed changeFeed;
    private AtomicInteger sequence = null;
    private GraphDatabaseService database;

    public ChangeFeedModule(String moduleId, ChangeFeedConfiguration configuration, GraphDatabaseService database) {
        super(moduleId);
        this.configuration = configuration;
        this.database = database;
        this.changeFeed = new ChangeFeed(database);
    }

    @Override
    public void initialize(GraphDatabaseService database) {
        try (Transaction tx = database.beginTx()) {
            Node result = getSingleOrNull(at(database).getAllNodesWithLabel(Labels._GA_ChangeFeed));
            if (result == null) {
                LOG.info("Creating the _GA_ChangeFeed root");
                database.createNode(Labels._GA_ChangeFeed);
            }
            tx.success();
        }
        LOG.info("Initialized ChangeFeedModule");
        super.initialize(database);
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
        if (sequence == null) { //Initialize the sequence if not already done.
            synchronized (this) {
                if (sequence == null) {
                    int startSequence = 0;
                    Node result = getSingleOrNull(at(database).getAllNodesWithLabel(Labels._GA_ChangeFeed));
                    if (result != null) {
                        Relationship nextRel = result.getSingleRelationship(Relationships.GA_CHANGEFEED_NEXT_CHANGE, Direction.OUTGOING);
                        if (nextRel != null) {
                            startSequence = (Integer) nextRel.getEndNode().getProperty("sequence");
                        }
                    }
                    sequence = new AtomicInteger(startSequence);
                }
            }
        }


        if (transactionData.mutationsOccurred()) {
            ChangeSet changeSet = new ChangeSet();
            changeSet.getChanges().addAll(transactionData.mutationsToStrings());
            changeSet.setSequence(sequence.incrementAndGet()); //TODO might this result in holes if a runtime exception is thrown at the end of this module or any other
            changeFeed.recordChange(changeSet);
        }

        return null;
    }

    @Override
    public NodeBasedContext createInitialContext(GraphDatabaseService database) {
        Node changeRoot = getSingleOrNull(at(database).getAllNodesWithLabel(Labels._GA_ChangeFeed));
        LOG.info("Pruning Initial context created");
        return new NodeBasedContext(changeRoot);
    }

    @Override
    public NodeBasedContext doSomeWork(NodeBasedContext lastContext, GraphDatabaseService database) {
        Node changeRoot;
        try {
            changeRoot = lastContext.find(database);
        } catch (NotFoundException e) {
            changeRoot = getSingleOrNull(at(database).getAllNodesWithLabel(Labels._GA_ChangeFeed));
        }

        pruneChangeFeed(changeRoot);

        return new NodeBasedContext(changeRoot, System.currentTimeMillis() + PRUNE_DELAY);
    }

    private void pruneChangeFeed(Node changeRoot) {
        final int MAX_FEED_LENGTH_EXCEEDED = 3;

        Relationship oldestChangeRel = changeRoot.getSingleRelationship(Relationships.GA_CHANGEFEED_OLDEST_CHANGE, Direction.OUTGOING);
        if (oldestChangeRel != null) {
            Node oldestNode = oldestChangeRel.getEndNode();
            Node newestNode = changeRoot.getSingleRelationship(Relationships.GA_CHANGEFEED_NEXT_CHANGE, Direction.OUTGOING).getEndNode();
            if (newestNode != null) {
                int highSequence = (int) newestNode.getProperty("sequence");
                int lowSequence = (int) oldestNode.getProperty("sequence");
                int nodesToDelete = ((highSequence - lowSequence) + 1) - configuration.getMaxChanges();
                Node newOldestNode = null;
                if (nodesToDelete > MAX_FEED_LENGTH_EXCEEDED) {
                    LOG.info("Preparing to prune change feed by deleting {} nodes", nodesToDelete);
                    changeRoot.getSingleRelationship(Relationships.GA_CHANGEFEED_OLDEST_CHANGE, Direction.OUTGOING).delete();
                    while (nodesToDelete > 0) {
                        Relationship rel = oldestNode.getSingleRelationship(Relationships.GA_CHANGEFEED_NEXT_CHANGE, Direction.INCOMING);
                        newOldestNode = rel.getStartNode();
                        rel.delete();
                        oldestNode.delete();
                        oldestNode = newOldestNode;
                        nodesToDelete--;
                    }
                    changeRoot.createRelationshipTo(newOldestNode, Relationships.GA_CHANGEFEED_OLDEST_CHANGE);
                    LOG.info("_GA_ChangeFeed pruning complete");
                }
            }
        }
    }
}
