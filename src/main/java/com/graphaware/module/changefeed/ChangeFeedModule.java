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

import static com.graphaware.module.changefeed.Properties.SEQUENCE;
import static com.graphaware.module.changefeed.Relationships.GA_CHANGEFEED_NEXT_CHANGE;
import static com.graphaware.module.changefeed.Relationships.GA_CHANGEFEED_OLDEST_CHANGE;
import static org.neo4j.graphdb.Direction.INCOMING;
import static org.neo4j.graphdb.Direction.OUTGOING;

/**
 * A {@link com.graphaware.runtime.module.TxDrivenModule} that keeps track of changes in the graph.
 * Also implements {@link TimerDrivenModule} to perform pruning of old changes.
 */
public class ChangeFeedModule extends BaseTxDrivenModule<Void> implements TimerDrivenModule<EmptyContext> {

    private static final int PRUNE_DELAY = 5000;

    private final ChangeFeedConfiguration configuration;
    private final GraphChangeRepository repository;

    public ChangeFeedModule(String moduleId, ChangeFeedConfiguration configuration, GraphDatabaseService database) {
        super(moduleId);
        this.configuration = configuration;
        this.repository = new GraphChangeRepository(database);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void start(GraphDatabaseService database) {
        repository.initialize();
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
        repository.recordChanges(transactionData.mutationsToStrings());
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
        repository.pruneChanges(configuration.getMaxChanges());
        return new EmptyContext(System.currentTimeMillis() + PRUNE_DELAY);
    }
}
