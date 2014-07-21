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

import com.graphaware.module.changefeed.cache.CachingGraphChangeReader;
import com.graphaware.module.changefeed.domain.ChangeSet;
import com.graphaware.module.changefeed.domain.Labels;
import com.graphaware.module.changefeed.domain.Relationships;
import com.graphaware.module.changefeed.io.ChangeReader;
import com.graphaware.runtime.GraphAwareRuntime;
import com.graphaware.runtime.GraphAwareRuntimeFactory;
import com.graphaware.test.integration.DatabaseIntegrationTest;
import org.junit.Test;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.graphdb.event.TransactionEventHandler;

import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.graphaware.common.util.IterableUtils.getSingleOrNull;
import static com.graphaware.module.changefeed.domain.Properties.SEQUENCE;
import static org.junit.Assert.*;
import static org.neo4j.tooling.GlobalGraphOperations.at;

/**
 * Tests the module in an embedded db programmatically
 */
public class ChangeFeedModuleEmbeddedProgrammaticIntegrationTest extends DatabaseIntegrationTest {

    private ChangeReader changeReader;

    private void registerSingleModuleAndCreateReader() {
        GraphAwareRuntime runtime = GraphAwareRuntimeFactory.createRuntime(getDatabase());
        runtime.registerModule(new ChangeFeedModule("CFM", new ChangeFeedConfiguration(3), getDatabase()));
        runtime.start();

        changeReader = new CachingGraphChangeReader(getDatabase());
    }

    @Test
    public void feedShouldBeEmptyOnANewDatabase() {
        registerSingleModuleAndCreateReader();

        assertEquals(0, changeReader.getAllChanges().size());
    }

    @Test
    public void changeRootShouldHavePointerToOldestChange() {
        registerSingleModuleAndCreateReader();

        Node node1, node2;
        try (Transaction tx = getDatabase().beginTx()) {
            node1 = getDatabase().createNode();
            node1.setProperty("name", "MB");
            tx.success();
        }

        try (Transaction tx = getDatabase().beginTx()) {
            node2 = getDatabase().createNode();
            node2.setProperty("name", "GraphAware");
            tx.success();
        }

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
    public void graphChangesShouldAppearInTheChangeFeed() {
        registerSingleModuleAndCreateReader();

        Node node1, node2;
        try (Transaction tx = getDatabase().beginTx()) {
            node1 = getDatabase().createNode();
            node1.setProperty("name", "MB");
            node1.addLabel(DynamicLabel.label("Person"));
            node2 = getDatabase().createNode();
            node2.addLabel(DynamicLabel.label("Company"));
            node1.createRelationshipTo(node2, DynamicRelationshipType.withName("WORKS_AT"));

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

        Collection<ChangeSet> changes = changeReader.getAllChanges();
        assertEquals(3, changes.size());
        Iterator<ChangeSet> it = changes.iterator();
        ChangeSet set1 = it.next();
        long set1Date = set1.getTimestamp();
        assertEquals(2, set1.getChanges().size());
        assertTrue(set1.getChanges().contains("Changed node (:Company {location: London, name: GraphAware}) to ({name: GraphAware})"));
        assertTrue(set1.getChanges().contains("Changed node (:Person {name: MB}) to (:Person {name: Michal})"));
        assertEquals(set1.getSequence(), 3);

        ChangeSet set2 = it.next();
        long set2Date = set2.getTimestamp();
        assertEquals(1, set2.getChanges().size());
        assertTrue(set2.getChanges().contains("Changed node (:Company) to (:Company {location: London, name: GraphAware})"));
        assertEquals(2, set2.getSequence());

        ChangeSet set3 = it.next();
        long set3Date = set3.getTimestamp();
        assertEquals(3, set3.getChanges().size());
        assertTrue(set3.getChanges().contains("Created node (:Company)"));
        assertTrue(set3.getChanges().contains("Created node (:Person {name: MB})"));
        assertTrue(set3.getChanges().contains("Created relationship (:Person {name: MB})-[:WORKS_AT]->(:Company)"));
        assertEquals(1, set3.getSequence());

        assertTrue(set1Date >= set2Date);
        assertTrue(set2Date >= set3Date);

    }

    @Test
    public void changeFeedSizeShouldNotBeExceeded() {
        registerSingleModuleAndCreateReader();

        Node node1, node2;
        try (Transaction tx = getDatabase().beginTx()) {
            node1 = getDatabase().createNode();
            node1.setProperty("name", "MB");
            node1.addLabel(DynamicLabel.label("Person"));
            node2 = getDatabase().createNode();
            node2.addLabel(DynamicLabel.label("Company"));

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

        try (Transaction tx = getDatabase().beginTx()) {
            node2.delete();
            tx.success();
        }
        Collection<ChangeSet> changes = changeReader.getNumberOfChanges(3);
        assertEquals(3, changes.size());
        Iterator<ChangeSet> it = changes.iterator();

        ChangeSet set1 = it.next();
        long set1Date = set1.getTimestamp();
        assertEquals(1, set1.getChanges().size());
        assertTrue(set1.getChanges().contains("Deleted node ({name: GraphAware})"));


        ChangeSet set2 = it.next();
        long set2Date = set2.getTimestamp();
        assertEquals(2, set2.getChanges().size());
        assertTrue(set2.getChanges().contains("Changed node (:Company {location: London, name: GraphAware}) to ({name: GraphAware})"));
        assertTrue(set2.getChanges().contains("Changed node (:Person {name: MB}) to (:Person {name: Michal})"));


        ChangeSet set3 = it.next();
        long set3Date = set3.getTimestamp();
        assertEquals(1, set3.getChanges().size());
        assertTrue(set3.getChanges().contains("Changed node (:Company) to (:Company {location: London, name: GraphAware})"));

        assertTrue(set1Date >= set2Date);
        assertTrue(set2Date >= set3Date);
    }

    @Test
    public void changesSinceShouldReturnOnlyChangesSinceTheSequenceProvided() {
        registerSingleModuleAndCreateReader();

        Node node1, node2;
        try (Transaction tx = getDatabase().beginTx()) {
            node1 = getDatabase().createNode();
            node1.setProperty("name", "MB");
            node1.addLabel(DynamicLabel.label("Person"));
            node2 = getDatabase().createNode();
            node2.addLabel(DynamicLabel.label("Company"));

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

        try (Transaction tx = getDatabase().beginTx()) {
            node2.delete();
            tx.success();
        }
        Collection<ChangeSet> changes = changeReader.getNumberOfChanges(3);
        assertEquals(3, changes.size());

        Iterator<ChangeSet> it = changes.iterator();
        changes = changeReader.getChangesSince(2);
        assertEquals(2, changes.size());

        ChangeSet set1 = it.next();
        assertEquals(1, set1.getChanges().size());
        assertTrue(set1.getChanges().contains("Deleted node ({name: GraphAware})"));
        assertEquals(4, set1.getSequence());

        ChangeSet set2 = it.next();
        assertEquals(2, set2.getChanges().size());
        assertTrue(set2.getChanges().contains("Changed node (:Company {location: London, name: GraphAware}) to ({name: GraphAware})"));
        assertTrue(set2.getChanges().contains("Changed node (:Person {name: MB}) to (:Person {name: Michal})"));
        assertEquals(3, set2.getSequence());

    }

    @Test
    public void transactionsNotCommittedShouldNotReflectInTheChangeFeed() {
        registerSingleModuleAndCreateReader();

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
            node1.createRelationshipTo(node2, DynamicRelationshipType.withName("WORKS_AT"));

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

        Collection<ChangeSet> changes = changeReader.getAllChanges();
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
    public void sequenceNumbersShouldBeOrdered() throws InterruptedException {
        registerSingleModuleAndCreateReader();

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

        Collection<ChangeSet> changes = changeReader.getAllChanges();
        assertEquals(2, changes.size());
        Iterator<ChangeSet> it = changes.iterator();

        assertEquals(2, it.next().getSequence());
        assertEquals(1, it.next().getSequence());
    }
    
    @Test
    public void shouldBeAbleToRegisterMultipleModules() {
        
    }
    
    @Test
    public void whenTransactionFailsChangesToNotAppear() {
        
    }
}
