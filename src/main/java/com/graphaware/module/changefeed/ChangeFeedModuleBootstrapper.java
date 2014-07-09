package com.graphaware.module.changefeed;

import com.graphaware.runtime.module.RuntimeModule;
import com.graphaware.runtime.module.RuntimeModuleBootstrapper;
import org.neo4j.graphdb.GraphDatabaseService;

import java.util.Map;

/**
 * Bootstraps the {@link ChangeFeedModule}
 */
public class ChangeFeedModuleBootstrapper implements RuntimeModuleBootstrapper {

    @Override
    public RuntimeModule bootstrapModule(String moduleId, Map<String, String> config, GraphDatabaseService database) {
        return new ChangeFeedModule(moduleId,database,config);
    }
}
