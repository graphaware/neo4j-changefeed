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

import com.graphaware.common.strategy.NodeInclusionStrategy;
import com.graphaware.common.strategy.RelationshipInclusionStrategy;
import com.graphaware.common.util.IterableUtils;
import com.graphaware.runtime.config.MinimalTxDrivenModuleConfiguration;
import com.graphaware.runtime.config.TxDrivenModuleConfiguration;
import com.graphaware.runtime.module.BaseTxDrivenModule;
import com.graphaware.runtime.module.TimerDrivenModule;
import com.graphaware.runtime.strategy.InclusionStrategiesFactory;
import com.graphaware.tx.event.improved.api.ImprovedTransactionData;
import org.neo4j.graphdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static com.graphaware.common.util.IterableUtils.getSingleOrNull;
import static org.neo4j.tooling.GlobalGraphOperations.at;

/**
 * A GraphAware Transaction Driven runtime module that keeps track of changes in the graph
 */
public class ChangeFeedModule extends BaseTxDrivenModule<Void> implements TimerDrivenModule<PruningNodeContext> {

    private static final int MAX_CHANGES_DEFAULT = 50;
    private static int maxChanges = MAX_CHANGES_DEFAULT;
    private static final Logger LOG = LoggerFactory.getLogger(ChangeFeedModule.class);
    private static final Object mutex = new Object();
    private final TxDrivenModuleConfiguration configuration;

    private final ChangeFeed changeFeed;
    private AtomicInteger sequence = null;
    private GraphDatabaseService database;

    public ChangeFeedModule(String moduleId, GraphDatabaseService database, Map<String, String> config) {
        super(moduleId);
        if (config.get("maxChanges") != null) {
            maxChanges = Integer.parseInt(config.get("maxChanges"));
            LOG.info("MaxChanges set to {}", maxChanges);
        }
        this.database = database;
        this.changeFeed = new ChangeFeed(database);

        //Do not record changes for nodes and relationships that belong to the changefeed
        configuration = new MinimalTxDrivenModuleConfiguration(
                InclusionStrategiesFactory.allBusiness()
                        .with(
                                new RelationshipInclusionStrategy() {
                                    @Override
                                    public boolean include(Relationship rel) {
                                        return !(rel.isType(Relationships.NEXT_CHANGE) || rel.isType(Relationships.OLDEST_CHANGE));
                                    }
                                }
                        )
                        .with(
                                new NodeInclusionStrategy() {
                                    @Override
                                    public boolean include(Node node) {
                                        return !(IterableUtils.contains(node.getLabels(), Labels.ChangeSet) || IterableUtils.contains(node.getLabels(), Labels.ChangeFeed));
                                    }
                                }
                        ));
    }

    /**
     * Get the maximum number of changes to be maintained in the feed
     *
     * @return max changes
     */
    public static int getMaxChanges() {
        return maxChanges;
    }

    @Override
    public void initialize(GraphDatabaseService database) {
        try (Transaction tx = database.beginTx()) {
            Node result = getSingleOrNull(at(database).getAllNodesWithLabel(Labels.ChangeFeed));
            if (result == null) {
                LOG.info("Creating the ChangeFeed root");
                database.createNode(Labels.ChangeFeed);
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

    @Override
    public Void beforeCommit(ImprovedTransactionData transactionData) {
        if (sequence == null) { //Initialize the sequence if not already done.
            synchronized (this) {
                if (sequence == null) {
                    int startSequence = 0;
                    Node result = getSingleOrNull(at(database).getAllNodesWithLabel(Labels.ChangeFeed));
                    if (result != null) {
                        Relationship nextRel = result.getSingleRelationship(Relationships.NEXT_CHANGE, Direction.OUTGOING);
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
    public void afterCommit(Void state) {

    }

    @Override
    public PruningNodeContext createInitialContext(GraphDatabaseService database) {
        Node changeRoot = getSingleOrNull(at(database).getAllNodesWithLabel(Labels.ChangeFeed));
        LOG.info("Pruning Initial context created");
        return new PruningNodeContext(changeRoot);
    }

    @Override
    public PruningNodeContext doSomeWork(PruningNodeContext lastContext, GraphDatabaseService database) {

        Node changeRoot = lastContext.find(database);

        //State maintenance necessary as a workaround for timer modules being invoked even if the previous work is incomplete
        synchronized (mutex) {
            switch (lastContext.getState()) {
                case RUNNING:
                    return lastContext;
                case NOT_RUNNING:
                    lastContext.setState(PruningNodeContext.State.INIT);
                    return lastContext;
                case INIT:
                    lastContext.setState(PruningNodeContext.State.RUNNING);
            }
        }

        pruneChangeFeed(changeRoot);
        lastContext.setState(PruningNodeContext.State.NOT_RUNNING);
        return lastContext;
    }

    private void pruneChangeFeed(Node changeRoot) {
        final int MAX_FEED_LENGTH_EXCEEDED = 3;

        Relationship oldestChangeRel = changeRoot.getSingleRelationship(Relationships.OLDEST_CHANGE, Direction.OUTGOING);
        if (oldestChangeRel != null) {
            Node oldestNode = oldestChangeRel.getEndNode();
            Node newestNode = changeRoot.getSingleRelationship(Relationships.NEXT_CHANGE, Direction.OUTGOING).getEndNode();
            if (newestNode != null) {
                int highSequence = (int) newestNode.getProperty("sequence");
                int lowSequence = (int) oldestNode.getProperty("sequence");
                int nodesToDelete = ((highSequence - lowSequence) + 1) - maxChanges;
                Node newOldestNode = null;
                if (nodesToDelete > MAX_FEED_LENGTH_EXCEEDED) {
                    LOG.info("Preparing to prune change feed by deleting {} nodes", nodesToDelete);
                    changeRoot.getSingleRelationship(Relationships.OLDEST_CHANGE, Direction.OUTGOING).delete();
                    while (nodesToDelete > 0) {
                        Relationship rel = oldestNode.getSingleRelationship(Relationships.NEXT_CHANGE, Direction.INCOMING);
                        newOldestNode = rel.getStartNode();
                        rel.delete();
                        oldestNode.delete();
                        oldestNode = newOldestNode;
                        nodesToDelete--;
                    }
                    changeRoot.createRelationshipTo(newOldestNode, Relationships.OLDEST_CHANGE);
                    LOG.info("ChangeFeed pruning complete");
                }
            }
        }
    }
}
