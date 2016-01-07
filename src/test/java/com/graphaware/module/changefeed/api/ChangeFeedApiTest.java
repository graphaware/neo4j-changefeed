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

package com.graphaware.module.changefeed.api;

import com.graphaware.module.changefeed.ChangeFeedConfiguration;
import com.graphaware.module.changefeed.ChangeFeedModule;
import com.graphaware.module.changefeed.domain.ChangeSet;
import com.graphaware.module.changefeed.util.UuidUtil;
import com.graphaware.runtime.GraphAwareRuntime;
import com.graphaware.runtime.GraphAwareRuntimeFactory;
import com.graphaware.runtime.config.FluentRuntimeConfiguration;
import com.graphaware.runtime.config.RuntimeConfiguration;
import com.graphaware.runtime.schedule.FixedDelayTimingStrategy;
import com.graphaware.runtime.schedule.TimingStrategy;
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
    private List<String> uuids;

    @Override
    public void setUp() throws Exception {
        super.setUp();

        uuids = new ArrayList<>();

        TimingStrategy timingStrategy = FixedDelayTimingStrategy
                .getInstance()
                .withInitialDelay(100)
                .withDelay(100);

        RuntimeConfiguration runtimeConfiguration = FluentRuntimeConfiguration
                .defaultConfiguration()
                .withTimingStrategy(timingStrategy);

        GraphAwareRuntime runtime = GraphAwareRuntimeFactory.createRuntime(getDatabase(), runtimeConfiguration);
        runtime.registerModule(new ChangeFeedModule("CFM",
                ChangeFeedConfiguration
                        .defaultConfiguration()
                        .withMaxChanges(3)
                        .withPruneDelay(200),
                getDatabase()));

        runtime.start();

        engine = new ExecutionEngine(getDatabase());

        engine.execute("CREATE (michal:Person {name:'Michal'})");
        uuids.add(UuidUtil.getUuidOfLatestChange(getDatabase()));

        engine.execute("CREATE (luanne:Person {name:'Luanne'})");
        uuids.add(UuidUtil.getUuidOfLatestChange(getDatabase()));


        engine.execute("CREATE (ga:Company {name:'GraphAware'})");
        uuids.add(UuidUtil.getUuidOfLatestChange(getDatabase()));


        engine.execute("MATCH (michal:Person {name:'Michal'}), (ga:Company {name:'GraphAware'}), (luanne:Person {name:'Luanne'}) " +
                "MERGE (michal)-[:WORKS_FOR]->(ga)<-[:WORKS_FOR]-(luanne)");
        uuids.add(UuidUtil.getUuidOfLatestChange(getDatabase()));


        api = new ChangeFeedApi(getDatabase());
    }

    @Test
    public void shouldReturnCorrectNumberOfResultsWhenAskedForAll() {
        List<ChangeSet> result = new ArrayList<>(api.getChangeFeed(null, null));
        assertEquals(3, result.size());

        assertEquals(uuids.get(3), result.get(0).getUuid());
        assertEquals(uuids.get(2), result.get(1).getUuid());
        assertEquals(uuids.get(1), result.get(2).getUuid());

        assertEquals("{Created relationship (:Person {name: Michal})-[:WORKS_FOR]->(:Company {name: GraphAware}),Created relationship (:Person {name: Luanne})-[:WORKS_FOR]->(:Company {name: GraphAware})}", ArrayUtils.toString(result.get(0).getChangesAsArray()));
        assertEquals("{Created node (:Company {name: GraphAware})}", ArrayUtils.toString(result.get(1).getChangesAsArray()));
        assertEquals("{Created node (:Person {name: Luanne})}", ArrayUtils.toString(result.get(2).getChangesAsArray()));
    }

    @Test
    public void shouldReturnCorrectNumberOfResultsWhenAskedWithLimit() {
        api = new ChangeFeedApi(getDatabase());

        List<ChangeSet> result = new ArrayList<>(api.getChangeFeed("CFM", null, 2));
        assertEquals(2, result.size());

        assertEquals(uuids.get(3), result.get(0).getUuid());
        assertEquals(uuids.get(2), result.get(1).getUuid());
    }

    @Test
    public void shouldReturnCorrectNumberOfResultsWhenAskedSince() {
        List<ChangeSet> result = new ArrayList<>(api.getChangeFeed(uuids.get(1), null));
        assertEquals(2, result.size());

        assertEquals(uuids.get(3), result.get(0).getUuid());
        assertEquals(uuids.get(2), result.get(1).getUuid());
    }

    @Test
    public void shouldReturnCorrectNumberOfResultsWhenAskedSinceWithLimit() {
        List<ChangeSet> result = new ArrayList<>(api.getChangeFeed(uuids.get(2), 1));
        assertEquals(1, result.size());

        assertEquals(uuids.get(3), result.get(0).getUuid());
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

        Thread.sleep(300);

        try (Transaction tx = getDatabase().beginTx()) {
            assertEquals(3, count(at(getDatabase()).getAllNodesWithLabel(_GA_ChangeSet)));
            tx.success();
        }
    }

}
