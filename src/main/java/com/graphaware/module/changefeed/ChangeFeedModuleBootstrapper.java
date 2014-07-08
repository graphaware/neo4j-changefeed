package com.graphaware.module.changefeed;

import com.graphaware.runtime.GraphAwareRuntimeModule;
import com.graphaware.runtime.GraphAwareRuntimeModuleBootstrapper;
import org.neo4j.graphdb.GraphDatabaseService;

import java.util.Map;

/**
 * Bootstraps the {@link ChangeFeedModule}
 */
public class ChangeFeedModuleBootstrapper implements GraphAwareRuntimeModuleBootstrapper {

    @Override
    public GraphAwareRuntimeModule bootstrapModule(String moduleId, Map<String, String> config, GraphDatabaseService database) {
        return new ChangeFeedModule(moduleId,database,config);
    }
}
