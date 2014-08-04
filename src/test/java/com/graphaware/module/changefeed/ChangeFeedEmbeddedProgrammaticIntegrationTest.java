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
import com.graphaware.module.changefeed.cache.CachingGraphChangeReader;
import com.graphaware.module.changefeed.domain.ChangeSet;
import com.graphaware.module.changefeed.domain.Labels;
import com.graphaware.module.changefeed.domain.Relationships;
import com.graphaware.module.changefeed.io.GraphChangeReader;
import com.graphaware.runtime.GraphAwareRuntime;
import com.graphaware.runtime.GraphAwareRuntimeFactory;
import com.graphaware.runtime.config.FluentRuntimeConfiguration;
import com.graphaware.runtime.config.RuntimeConfiguration;
import com.graphaware.runtime.schedule.FixedDelayTimingStrategy;
import com.graphaware.runtime.schedule.TimingStrategy;
import com.graphaware.test.integration.DatabaseIntegrationTest;
import org.junit.Test;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.graphdb.event.TransactionEventHandler;

import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.graphaware.common.util.IterableUtils.getSingleOrNull;
import static com.graphaware.module.changefeed.domain.Properties.*;
import static org.junit.Assert.*;
import static org.neo4j.graphdb.DynamicRelationshipType.withName;
import static org.neo4j.tooling.GlobalGraphOperations.at;

/**
 * Tests the module in an embedded db programmatically
 */
public class ChangeFeedEmbeddedProgrammaticIntegrationTest extends DatabaseIntegrationTest {

    @Test
    public void feedShouldBeEmptyOnANewDatabase() {
        registerSingleModuleAndStart();

        assertEquals(0, new GraphChangeReader(getDatabase()).getAllChanges().size());
        assertEquals(0, new GraphChangeReader(getDatabase(), "CFM").getAllChanges().size());
        assertEquals(0, new CachingGraphChangeReader(getDatabase()).getAllChanges().size());
        assertEquals(0, new CachingGraphChangeReader(getDatabase(), "CFM").getAllChanges().size());
    }

    @Test
    public void changeRootShouldHavePointerToOldestChange() {
        registerSingleModuleAndStart();

        performModifications();

        try (Transaction tx = getDatabase().beginTx()) {
            Node changeRoot = getSingleOrNull(at(getDatabase()).getAllNodesWithLabel(Labels._GA_ChangeFeed));
            assertNotNull(changeRoot);
            Relationship rel = changeRoot.getSingleRelationship(Relationships._GA_CHANGEFEED_OLDEST_CHANGE, Direction.OUTGOING);
            assertNotNull(rel);
            assertEquals(1L, rel.getEndNode().getProperty(SEQUENCE));
            tx.success();
        }
    }

    @Test
    public void graphChangesShouldAppearInTheChangeFeed() throws InterruptedException {
        registerSingleModuleAndStart();

        performModifications();

        Thread.sleep(1200); //wait for pruning

        verifyChanges(3, new GraphChangeReader(getDatabase()).getAllChanges());
        verifyChanges(3, new GraphChangeReader(getDatabase(), "CFM").getAllChanges());
        verifyChanges(3, new CachingGraphChangeReader(getDatabase()).getAllChanges());
        verifyChanges(3, new CachingGraphChangeReader(getDatabase(), "CFM").getAllChanges());

        verifyChanges(3, new GraphChangeReader(getDatabase()).getNumberOfChanges(3));
        verifyChanges(3, new GraphChangeReader(getDatabase(), "CFM").getNumberOfChanges(3));
        verifyChanges(3, new CachingGraphChangeReader(getDatabase()).getNumberOfChanges(3));
        verifyChanges(3, new CachingGraphChangeReader(getDatabase(), "CFM").getNumberOfChanges(3));

        verifyChanges(2, new GraphChangeReader(getDatabase()).getNumberOfChanges(2));
        verifyChanges(2, new GraphChangeReader(getDatabase(), "CFM").getNumberOfChanges(2));
        verifyChanges(2, new CachingGraphChangeReader(getDatabase()).getNumberOfChanges(2));
        verifyChanges(2, new CachingGraphChangeReader(getDatabase(), "CFM").getNumberOfChanges(2));

        verifyChanges(3, new GraphChangeReader(getDatabase()).getChangesSince(0));
        verifyChanges(3, new GraphChangeReader(getDatabase(), "CFM").getChangesSince(0));
        verifyChanges(3, new CachingGraphChangeReader(getDatabase()).getChangesSince(0));
        verifyChanges(3, new CachingGraphChangeReader(getDatabase(), "CFM").getChangesSince(0));

        verifyChanges(2, new GraphChangeReader(getDatabase()).getChangesSince(2));
        verifyChanges(2, new GraphChangeReader(getDatabase(), "CFM").getChangesSince(2));
        verifyChanges(2, new CachingGraphChangeReader(getDatabase()).getChangesSince(2));
        verifyChanges(2, new CachingGraphChangeReader(getDatabase(), "CFM").getChangesSince(2));

        verifyChanges(3, new GraphChangeReader(getDatabase()).getNumberOfChangesSince(0, 3));
        verifyChanges(3, new GraphChangeReader(getDatabase(), "CFM").getNumberOfChangesSince(0, 3));
        verifyChanges(3, new CachingGraphChangeReader(getDatabase()).getNumberOfChangesSince(0, 3));
        verifyChanges(3, new CachingGraphChangeReader(getDatabase(), "CFM").getNumberOfChangesSince(0, 3));

        verifyChanges(1, new GraphChangeReader(getDatabase()).getNumberOfChangesSince(2, 1));
        verifyChanges(1, new GraphChangeReader(getDatabase(), "CFM").getNumberOfChangesSince(3, 3));
        verifyChanges(1, new CachingGraphChangeReader(getDatabase()).getNumberOfChangesSince(3, 3));
        verifyChanges(1, new CachingGraphChangeReader(getDatabase(), "CFM").getNumberOfChangesSince(2, 1));
    }

    private void performModifications() {
        try (Transaction tx = getDatabase().beginTx()) {
            getDatabase().createNode().setProperty("name", "will not appear in changefeed");
            tx.success();
        }

        Node node1, node2;
        try (Transaction tx = getDatabase().beginTx()) {
            node1 = getDatabase().createNode();
            node1.setProperty("name", "MB");
            node1.addLabel(DynamicLabel.label("Person"));
            node2 = getDatabase().createNode();
            node2.addLabel(DynamicLabel.label("Company"));
            node1.createRelationshipTo(node2, withName("WORKS_AT"));

            tx.success();
        }

        try (Transaction tx = getDatabase().beginTx()) {
            node2.setProperty("name", "GraphAware");
            node2.setProperty("location", "London");
            tx.success();
        }

        try (Transaction tx = getDatabase().beginTx()) {
            node1.setProperty("name", "Michal");
            node2.removeProperty("location");
            node2.removeLabel(DynamicLabel.label("Company"));
            tx.success();
        }
    }

    private void verifyChanges(int number, Collection<ChangeSet> changes) {
        assertEquals(number, changes.size());

        if (number < 1) {
            return;
        }

        Iterator<ChangeSet> it = changes.iterator();

        ChangeSet set1 = it.next();
        long set1Date = set1.getTimestamp();
        assertEquals(2, set1.getChanges().size());
        assertTrue(set1.getChanges().contains("Changed node (:Company {location: London, name: GraphAware}) to ({name: GraphAware})"));
        assertTrue(set1.getChanges().contains("Changed node (:Person {name: MB}) to (:Person {name: Michal})"));
        assertEquals(4, set1.getSequence());

        if (number < 2) {
            return;
        }

        ChangeSet set2 = it.next();
        long set2Date = set2.getTimestamp();
        assertEquals(1, set2.getChanges().size());
        assertTrue(set2.getChanges().contains("Changed node (:Company) to (:Company {location: London, name: GraphAware})"));
        assertEquals(3, set2.getSequence());

        if (number < 3) {
            return;
        }

        ChangeSet set3 = it.next();
        long set3Date = set3.getTimestamp();
        assertEquals(3, set3.getChanges().size());
        assertTrue(set3.getChanges().contains("Created node (:Company)"));
        assertTrue(set3.getChanges().contains("Created node (:Person {name: MB})"));
        assertTrue(set3.getChanges().contains("Created relationship (:Person {name: MB})-[:WORKS_AT]->(:Company)"));
        assertEquals(2, set3.getSequence());

        assertTrue(set1Date >= set2Date);
        assertTrue(set2Date >= set3Date);
    }

    @Test
    public void transactionsNotCommittedShouldNotReflectInTheChangeFeed() {
        registerSingleModuleAndStart();

        performChangesWithException();

        Collection<ChangeSet> changes = new GraphChangeReader(getDatabase()).getAllChanges();

        assertEquals(2, changes.size());
        Iterator<ChangeSet> it = changes.iterator();

        ChangeSet set1 = it.next();
        long set1Date = set1.getTimestamp();
        assertEquals(1, set1.getChanges().size());
        assertTrue(set1.getChanges().contains("Changed node (:Person {name: MB}) to (:Person {name: Michal})"));
        assertEquals(set1.getSequence(), 2);

        ChangeSet set2 = it.next();
        long set2Date = set2.getTimestamp();
        assertEquals(3, set2.getChanges().size());
        assertTrue(set2.getChanges().contains("Created node (:Company)"));
        assertTrue(set2.getChanges().contains("Created node (:Person {name: MB})"));
        assertTrue(set2.getChanges().contains("Created relationship (:Person {name: MB})-[:WORKS_AT]->(:Company)"));
        assertEquals(1, set2.getSequence());

        assertTrue(set1Date >= set2Date);
    }

    @Test
    public void transactionsNotCommittedShouldNotReflectInCachedChangeFeed() {
        registerSingleModuleAndStart();

        performChangesWithException();

        Collection<ChangeSet> changes = new CachingGraphChangeReader(getDatabase()).getAllChanges();

        assertEquals(2, changes.size());
        Iterator<ChangeSet> it = changes.iterator();

        ChangeSet set1 = it.next();
        long set1Date = set1.getTimestamp();
        assertEquals(1, set1.getChanges().size());
        assertTrue(set1.getChanges().contains("Changed node (:Person {name: MB}) to (:Person {name: Michal})"));
        assertEquals(set1.getSequence(), 2);

        ChangeSet set2 = it.next();
        long set2Date = set2.getTimestamp();
        assertEquals(3, set2.getChanges().size());
        assertTrue(set2.getChanges().contains("Created node (:Company)"));
        assertTrue(set2.getChanges().contains("Created node (:Person {name: MB})"));
        assertTrue(set2.getChanges().contains("Created relationship (:Person {name: MB})-[:WORKS_AT]->(:Company)"));
        assertEquals(1, set2.getSequence());

        assertTrue(set1Date >= set2Date);
    }

    private void performChangesWithException() {
        Node node1, node2;

        try (Transaction tx = getDatabase().beginTx()) {
            getDatabase().schema()
                    .constraintFor(DynamicLabel.label("Person"))
                    .assertPropertyIsUnique("name")
                    .create();
            tx.success();
        }
        try (Transaction tx = getDatabase().beginTx()) {

            node1 = getDatabase().createNode();
            node1.setProperty("name", "MB");
            node1.addLabel(DynamicLabel.label("Person"));
            node2 = getDatabase().createNode();
            node2.addLabel(DynamicLabel.label("Company"));
            node1.createRelationshipTo(node2, withName("WORKS_AT"));

            tx.success();
        }

        try (Transaction tx = getDatabase().beginTx()) {
            node2.setProperty("name", "GraphAware");
            node2.setProperty("location", "London");
            tx.failure();
        }

        try (Transaction tx = getDatabase().beginTx()) {
            Node node3 = getDatabase().createNode();
            node3.setProperty("name", "MB");
            node3.addLabel(DynamicLabel.label("Person"));
            tx.success();
        } catch (Exception e) {
            //tx will fail due to unique constraint violation, swallow the exception and check the feed
        }

        try (Transaction tx = getDatabase().beginTx()) {
            node1.setProperty("name", "Michal");
            tx.success();
        }
    }

    @Test
    public void sequenceNumbersShouldBeOrdered() throws InterruptedException {
        registerSingleModuleAndStart();

        //slow down the first tx:
        getDatabase().registerTransactionEventHandler(new TransactionEventHandler.Adapter<Void>() {
            protected volatile boolean hasRun = false;

            @Override
            public Void beforeCommit(TransactionData data) throws Exception {
                if (!hasRun) {
                    hasRun = true;
                    Thread.sleep(100);
                }
                return null;
            }
        });

        ExecutorService executor = Executors.newFixedThreadPool(2);

        executor.submit(new Runnable() {
            @Override
            public void run() {
                try (Transaction tx = getDatabase().beginTx()) {
                    getDatabase().createNode().setProperty("name", "One");
                    tx.success();
                }
            }
        });

        Thread.sleep(10);

        executor.submit(new Runnable() {
            @Override
            public void run() {
                try (Transaction tx = getDatabase().beginTx()) {
                    getDatabase().createNode().setProperty("name", "Two");
                    tx.success();
                }
            }
        });

        executor.shutdown();
        executor.awaitTermination(1000, TimeUnit.MILLISECONDS);

        //normal
        Collection<ChangeSet> changes = new GraphChangeReader(getDatabase()).getAllChanges();
        assertEquals(2, changes.size());
        Iterator<ChangeSet> it = changes.iterator();

        assertEquals(2, it.next().getSequence());
        assertEquals(1, it.next().getSequence());

        //caching
        GraphChangeReader reader = new CachingGraphChangeReader(getDatabase());
        changes = reader.getAllChanges();
        assertEquals(2, changes.size());
        it = changes.iterator();

        assertEquals(2, it.next().getSequence());
        assertEquals(1, it.next().getSequence());
    }

    @Test
    public void shouldBeAbleToRegisterMultipleModules() {
        registerMultipleModulesAndStart();

        try (Transaction tx = getDatabase().beginTx()) {
            getDatabase().createNode(DynamicLabel.label("Person"));
            tx.success();
        }

        try (Transaction tx = getDatabase().beginTx()) {
            getDatabase().createNode(DynamicLabel.label("Company"));
            tx.success();
        }

        assertEquals(1, new CachingGraphChangeReader(getDatabase(), "CFM1").getAllChanges().size());
        assertEquals(1, new GraphChangeReader(getDatabase(), "CFM1").getAllChanges().size());

        assertEquals(2, new CachingGraphChangeReader(getDatabase(), "CFM2").getAllChanges().size());
        assertEquals(2, new GraphChangeReader(getDatabase(), "CFM2").getAllChanges().size());
    }

    @Test(expected = NotFoundException.class)
    public void readingCountsForUnregisteredModuleShouldThrowException() {
        registerMultipleModulesAndStart();

        new CachingGraphChangeReader(getDatabase()).getAllChanges();
    }

    @Test(expected = NotFoundException.class)
    public void readingCountsForUnregisteredModuleShouldThrowException2() {
        registerMultipleModulesAndStart();

        new GraphChangeReader(getDatabase()).getAllChanges();
    }

    @Test(expected = NotFoundException.class)
    public void readingCountsWhenRuntimeHasNotBeenStartedShouldThrowException() {
        new GraphChangeReader(getDatabase()).getAllChanges();
    }

    @Test(expected = NotFoundException.class)
    public void readingCountsWhenRuntimeHasNotBeenStartedShouldThrowException2() {
        new CachingGraphChangeReader(getDatabase()).getAllChanges();
    }

    @Test(expected = IllegalStateException.class)
    public void readingCountsWhenRuntimeHasNotBeenStartedShouldThrowException3() throws Exception {
        //previously, a root has been created:
        try (Transaction tx = getDatabase().beginTx()) {
            Node root = getDatabase().createNode(Labels._GA_ChangeFeed);
            root.setProperty(MODULE_ID, "CFM");
            tx.success();
        }

        new CachingGraphChangeReader(getDatabase()).getAllChanges();
    }

    @Test
    public void sequenceShouldStartWhereItLeftOff() {
        //previously, a root has been created:
        try (Transaction tx = getDatabase().beginTx()) {
            Node root = getDatabase().createNode(Labels._GA_ChangeFeed);
            root.setProperty(MODULE_ID, "CFM");
            Node change = getDatabase().createNode(Labels._GA_ChangeSet);
            change.setProperty(SEQUENCE, 100L);
            change.setProperty(TIMESTAMP, new Date().getTime());
            change.setProperty(CHANGES, new String[]{"dummy"});
            root.createRelationshipTo(change, Relationships._GA_CHANGEFEED_NEXT_CHANGE);
            tx.success();
        }

        registerSingleModuleAndStart();

        try (Transaction tx = getDatabase().beginTx()) {
            getDatabase().createNode();
            tx.success();
        }

        assertEquals(101, new CachingGraphChangeReader(getDatabase()).getAllChanges().iterator().next().getSequence());
        assertEquals(101, new GraphChangeReader(getDatabase()).getAllChanges().iterator().next().getSequence());
    }

    private void registerSingleModuleAndStart() {
        TimingStrategy timingStrategy = FixedDelayTimingStrategy
                .getInstance()
                .withDelay(100);

        RuntimeConfiguration runtimeConfig = FluentRuntimeConfiguration
                .defaultConfiguration()
                .withTimingStrategy(timingStrategy);

        GraphAwareRuntime runtime = GraphAwareRuntimeFactory.createRuntime(getDatabase(), runtimeConfig);

        ChangeFeedConfiguration configuration = ChangeFeedConfiguration
                .defaultConfiguration()
                .withMaxChanges(3)
                .withPruneDelay(200)
                .withPruneWhenMaxExceededBy(1);

        runtime.registerModule(new ChangeFeedModule("CFM", configuration, getDatabase()));

        runtime.start();
    }

    private void registerMultipleModulesAndStart() {
        GraphAwareRuntime runtime = GraphAwareRuntimeFactory.createRuntime(getDatabase());

        ChangeFeedConfiguration configuration1 = ChangeFeedConfiguration
                .defaultConfiguration()
                .with(new NodeInclusionStrategy() {
                    @Override
                    public boolean include(Node node) {
                        return node.hasLabel(DynamicLabel.label("Person"));
                    }
                });

        ChangeFeedConfiguration configuration2 = ChangeFeedConfiguration.defaultConfiguration();

        runtime.registerModule(new ChangeFeedModule("CFM1", configuration1, getDatabase()));
        runtime.registerModule(new ChangeFeedModule("CFM2", configuration2, getDatabase()));

        runtime.start();
    }
}
