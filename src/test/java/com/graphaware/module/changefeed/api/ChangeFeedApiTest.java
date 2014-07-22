package com.graphaware.module.changefeed.api;

import com.graphaware.module.changefeed.ChangeFeedConfiguration;
import com.graphaware.module.changefeed.ChangeFeedModule;
import com.graphaware.module.changefeed.domain.ChangeSet;
import com.graphaware.runtime.GraphAwareRuntime;
import com.graphaware.runtime.GraphAwareRuntimeFactory;
import com.graphaware.test.integration.DatabaseIntegrationTest;
import org.apache.commons.lang.ArrayUtils;
import org.junit.Test;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Transaction;

import java.util.ArrayList;
import java.util.List;

import static com.graphaware.common.util.IterableUtils.count;
import static com.graphaware.module.changefeed.domain.Labels._GA_ChangeSet;
import static org.junit.Assert.assertEquals;
import static org.neo4j.tooling.GlobalGraphOperations.at;

/**
 * Integration test for {@link ChangeFeedApi}.
 */
public class ChangeFeedApiTest extends DatabaseIntegrationTest {

    private ChangeFeedApi api;
    private ExecutionEngine engine;

    @Override
    public void setUp() throws Exception {
        super.setUp();

        GraphAwareRuntime runtime = GraphAwareRuntimeFactory.createRuntime(getDatabase());
        runtime.registerModule(new ChangeFeedModule("CFM",
                ChangeFeedConfiguration
                        .defaultConfiguration()
                        .withMaxChanges(3)
                        .withPruneDelay(500),
                getDatabase()));

        runtime.start();

        engine = new ExecutionEngine(getDatabase());

        engine.execute("CREATE (michal:Person {name:'Michal'})");
        engine.execute("CREATE (luanne:Person {name:'Luanne'})");
        engine.execute("CREATE (ga:Company {name:'GraphAware'})");
        engine.execute("MATCH (michal:Person {name:'Michal'}), (ga:Company {name:'GraphAware'}), (luanne:Person {name:'Luanne'}) " +
                "MERGE (michal)-[:WORKS_FOR]->(ga)<-[:WORKS_FOR]-(luanne)");

        api = new ChangeFeedApi(getDatabase());
    }

    @Test
    public void shouldReturnCorrectNumberOfResultsWhenAskedForAll() {
        List<ChangeSet> result = new ArrayList<>(api.getChangeFeed(null, null));
        assertEquals(3, result.size());

        assertEquals(4, result.get(0).getSequence());
        assertEquals(3, result.get(1).getSequence());
        assertEquals(2, result.get(2).getSequence());

        assertEquals("{Created relationship (:Person {name: Michal})-[:WORKS_FOR]->(:Company {name: GraphAware}),Created relationship (:Person {name: Luanne})-[:WORKS_FOR]->(:Company {name: GraphAware})}", ArrayUtils.toString(result.get(0).getChangesAsArray()));
        assertEquals("{Created node (:Company {name: GraphAware})}", ArrayUtils.toString(result.get(1).getChangesAsArray()));
        assertEquals("{Created node (:Person {name: Luanne})}", ArrayUtils.toString(result.get(2).getChangesAsArray()));
    }

    @Test
    public void shouldReturnCorrectNumberOfResultsWhenAskedWithLimit() {
        api = new ChangeFeedApi(getDatabase());

        List<ChangeSet> result = new ArrayList<>(api.getChangeFeed("CFM", null, 2));
        assertEquals(2, result.size());

        assertEquals(4, result.get(0).getSequence());
        assertEquals(3, result.get(1).getSequence());
    }

    @Test
    public void shouldReturnCorrectNumberOfResultsWhenAskedSince() {
        List<ChangeSet> result = new ArrayList<>(api.getChangeFeed(2, null));
        assertEquals(2, result.size());

        assertEquals(4, result.get(0).getSequence());
        assertEquals(3, result.get(1).getSequence());
    }

    @Test
    public void shouldReturnCorrectNumberOfResultsWhenAskedSinceWithLimit() {
        List<ChangeSet> result = new ArrayList<>(api.getChangeFeed(2, 1));
        assertEquals(1, result.size());

        assertEquals(4, result.get(0).getSequence());
    }

    @Test(expected = NotFoundException.class)
    public void shouldThrowExceptionWhenModuleNotRegistered() {
        api.getChangeFeed("unknown", null, null);
    }

    @Test
    public void pruningShouldHappen() throws InterruptedException {
        for (int i = 0; i < 10; i++) {
            engine.execute("CREATE (:Person {name:'Person" + i + "'})");
        }
        try (Transaction tx = getDatabase().beginTx()) {
            assertEquals(14, count(at(getDatabase()).getAllNodesWithLabel(_GA_ChangeSet)));
            tx.success();
        }

        Thread.sleep(1000);

        try (Transaction tx = getDatabase().beginTx()) {
            assertEquals(3, count(at(getDatabase()).getAllNodesWithLabel(_GA_ChangeSet)));
            tx.success();
        }
    }
}
