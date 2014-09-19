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

import com.graphaware.common.policy.NodeInclusionPolicy;
import com.graphaware.common.policy.NodePropertyInclusionPolicy;
import com.graphaware.common.policy.RelationshipInclusionPolicy;
import com.graphaware.common.policy.RelationshipPropertyInclusionPolicy;
import com.graphaware.runtime.config.function.StringToNodeInclusionPolicy;
import com.graphaware.runtime.config.function.StringToNodePropertyInclusionPolicy;
import com.graphaware.runtime.config.function.StringToRelationshipInclusionPolicy;
import com.graphaware.runtime.config.function.StringToRelationshipPropertyInclusionPolicy;
import com.graphaware.runtime.module.RuntimeModule;
import com.graphaware.runtime.module.RuntimeModuleBootstrapper;
import org.neo4j.graphdb.GraphDatabaseService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Bootstraps the {@link ChangeFeedModule} in server mode.
 */
public class ChangeFeedModuleBootstrapper implements RuntimeModuleBootstrapper {

    private static final Logger LOG = LoggerFactory.getLogger(ChangeFeedModuleBootstrapper.class);

    //keys to use when configuring using neo4j.properties
    private static final String MAX_CHANGES = "maxChanges";
    private static final String PRUNE_DELAY = "pruneDelay";
    private static final String PRUNE_WHEN_EXCEEDED = "pruneWhenExceeded";
    private static final String NODE = "node";
    private static final String NODE_PROPERTY = "node.property";
    private static final String RELATIONSHIP = "relationship";
    private static final String RELATIONSHIP_PROPERTY = "relationship.property";

    /**
     * {@inheritDoc}
     */
    @Override
    public RuntimeModule bootstrapModule(String moduleId, Map<String, String> config, GraphDatabaseService database) {
        ChangeFeedConfiguration configuration = ChangeFeedConfiguration.defaultConfiguration();

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

        if (config.get(NODE) != null) {
            NodeInclusionPolicy policy = StringToNodeInclusionPolicy.getInstance().apply(config.get(NODE));
            LOG.info("Node Inclusion Strategy set to {}", policy);
            configuration = configuration.with(policy);
        }

        if (config.get(NODE_PROPERTY) != null) {
            NodePropertyInclusionPolicy policy = StringToNodePropertyInclusionPolicy.getInstance().apply(config.get(NODE_PROPERTY));
            LOG.info("Node Property Inclusion Strategy set to {}", policy);
            configuration = configuration.with(policy);
        }

        if (config.get(RELATIONSHIP) != null) {
            RelationshipInclusionPolicy policy = StringToRelationshipInclusionPolicy.getInstance().apply(config.get(RELATIONSHIP));
            LOG.info("Relationship Inclusion Strategy set to {}", policy);
            configuration = configuration.with(policy);
        }

        if (config.get(RELATIONSHIP_PROPERTY) != null) {
            RelationshipPropertyInclusionPolicy policy = StringToRelationshipPropertyInclusionPolicy.getInstance().apply(config.get(RELATIONSHIP_PROPERTY));
            LOG.info("Relationship Property Inclusion Strategy set to {}", policy);
            configuration = configuration.with(policy);
        }

        return new ChangeFeedModule(moduleId, configuration, database);
    }
}
