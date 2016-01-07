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

import com.graphaware.runtime.module.BaseRuntimeModuleBootstrapper;
import com.graphaware.runtime.module.RuntimeModule;
import com.graphaware.runtime.module.RuntimeModuleBootstrapper;
import org.neo4j.graphdb.GraphDatabaseService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Bootstraps the {@link ChangeFeedModule} in server mode.
 */
public class ChangeFeedModuleBootstrapper extends BaseRuntimeModuleBootstrapper<ChangeFeedConfiguration> implements RuntimeModuleBootstrapper {

    private static final Logger LOG = LoggerFactory.getLogger(ChangeFeedModuleBootstrapper.class);

    //keys to use when configuring using neo4j.properties
    private static final String MAX_CHANGES = "maxChanges";
    private static final String PRUNE_DELAY = "pruneDelay";
    private static final String PRUNE_WHEN_EXCEEDED = "pruneWhenExceeded";

    /**
     * {@inheritDoc}
     */
    @Override
    protected ChangeFeedConfiguration defaultConfiguration() {
        return ChangeFeedConfiguration.defaultConfiguration();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected RuntimeModule doBootstrapModule(String moduleId, Map<String, String> config, GraphDatabaseService database, ChangeFeedConfiguration configuration) {
        if (config.get(MAX_CHANGES) != null) {
            int maxChanges = Integer.parseInt(config.get(MAX_CHANGES));
            LOG.info("MaxChanges set to {}", maxChanges);
            configuration = configuration.withMaxChanges(maxChanges);
        }

        if (config.get(PRUNE_DELAY) != null) {
            int pruneDelay = Integer.parseInt(config.get(PRUNE_DELAY));
            LOG.info("PruneDelay set to {}", pruneDelay);
            configuration = configuration.withPruneDelay(pruneDelay);
        }

        if (config.get(PRUNE_WHEN_EXCEEDED) != null) {
            int pruneWhenExceeded = Integer.parseInt(config.get(PRUNE_WHEN_EXCEEDED));
            LOG.info("PruneWhenExceeded set to {}", pruneWhenExceeded);
            configuration = configuration.withPruneWhenMaxExceededBy(pruneWhenExceeded);
        }

        return new ChangeFeedModule(moduleId, configuration, database);
    }
}
