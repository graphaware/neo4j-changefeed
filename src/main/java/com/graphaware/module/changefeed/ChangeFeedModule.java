/*
 * Copyright (c) 2013-2016 GraphAware
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

package com.graphaware.module.changefeed;

import com.graphaware.module.changefeed.cache.CachingGraphChangeWriter;
import com.graphaware.module.changefeed.cache.ChangeSetCache;
import com.graphaware.module.changefeed.io.GraphChangeReader;
import com.graphaware.module.changefeed.io.GraphChangeWriter;
import com.graphaware.runtime.config.TxAndTimerDrivenModuleConfiguration;
import com.graphaware.runtime.metadata.EmptyContext;
import com.graphaware.runtime.module.BaseTxDrivenModule;
import com.graphaware.runtime.module.DeliberateTransactionRollbackException;
import com.graphaware.runtime.module.TimerDrivenModule;
import com.graphaware.tx.event.improved.api.ImprovedTransactionData;
import org.neo4j.graphdb.GraphDatabaseService;

/**
 * A {@link com.graphaware.runtime.module.TxDrivenModule} that keeps track of changes in the graph.
 * Also implements {@link TimerDrivenModule} to perform pruning of old changes.
 */
public class ChangeFeedModule extends BaseTxDrivenModule<Void> implements TimerDrivenModule<EmptyContext> {

    public static final String DEFAULT_MODULE_ID = "CFM";

    private final ChangeFeedConfiguration configuration;
    private final GraphChangeWriter changeWriter;
    private final ChangeSetCache changesCache;

    public ChangeFeedModule(String moduleId, ChangeFeedConfiguration configuration, GraphDatabaseService database) {
        super(moduleId);
        this.configuration = configuration;
        this.changesCache = new ChangeSetCache(configuration.getMaxChanges());
        this.changeWriter = new CachingGraphChangeWriter(database, moduleId, changesCache);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void start(GraphDatabaseService database) {
        changeWriter.initialize();
        changesCache.populate(new GraphChangeReader(database, getId()).getAllChanges());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TxAndTimerDrivenModuleConfiguration getConfiguration() {
        return configuration;
    }

    /**
     * Get cache of changes.
     *
     * @return cache.
     */
    public ChangeSetCache getChangesCache() {
        return changesCache;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Void beforeCommit(ImprovedTransactionData transactionData) {
        if (transactionData.hasBeenDeleted(changeWriter.getRoot())) {
            throw new DeliberateTransactionRollbackException("Not allowed to delete change feed root!");
        }

        changeWriter.recordChanges(transactionData.mutationsToStrings());
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public EmptyContext createInitialContext(GraphDatabaseService database) {
        return new EmptyContext(System.currentTimeMillis() + configuration.getPruneDelay());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public EmptyContext doSomeWork(EmptyContext lastContext, GraphDatabaseService database) {
        changeWriter.pruneChanges(configuration.getMaxChanges(), configuration.getPruneWhenMaxExceededBy());
        return new EmptyContext(System.currentTimeMillis() + configuration.getPruneDelay());
    }
}
