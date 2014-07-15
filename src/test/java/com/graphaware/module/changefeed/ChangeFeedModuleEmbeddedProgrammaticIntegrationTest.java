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

import com.graphaware.runtime.GraphAwareRuntime;
import com.graphaware.runtime.GraphAwareRuntimeFactory;
import com.graphaware.runtime.metadata.EmptyContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.*;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.util.List;

import static com.graphaware.common.util.IterableUtils.getSingleOrNull;
import static com.graphaware.module.changefeed.Properties.SEQUENCE;
import static org.junit.Assert.*;
import static org.neo4j.tooling.GlobalGraphOperations.at;

/**
 * Tests the module in an embedded db programmatically
 */
public class ChangeFeedModuleEmbeddedProgrammaticIntegrationTest {

    private GraphDatabaseService database;
    private ChangeReader changeReader;
    private ChangeFeedModule module;

    @Before
    public void setUp() {
        database = new TestGraphDatabaseFactory().newImpermanentDatabase();
        GraphAwareRuntime runtime = GraphAwareRuntimeFactory.createRuntime(database);
        module = new ChangeFeedModule("CFM", new ChangeFeedConfiguration(3), database);
        runtime.registerModule(module);
        runtime.start();

        changeReader = new GraphChangeReader(database);
    }

    @After
    public void tearDown() {
        database.shutdown();
    }

    @Test
    public void feedShouldBeEmptyOnANewDatabase() {
        List<ChangeSet> changes = changeReader.getAllChanges();
        assertEquals(0, changes.size());
    }

    @Test
    public void changeRootShouldHavePointerToOldestChange() {
        Node node1, node2;
        try (Transaction tx = database.beginTx()) {
            node1 = database.createNode();
            node1.setProperty("name", "MB");
            tx.success();
        }

        try (Transaction tx = database.beginTx()) {
            node2 = database.createNode();
            node2.setProperty("name", "GraphAware");
            tx.success();
        }

        try (Transaction tx = database.beginTx()) {
            Node changeRoot = getSingleOrNull(at(database).getAllNodesWithLabel(Labels._GA_ChangeFeed));
            assertNotNull(changeRoot);
            Relationship rel = changeRoot.getSingleRelationship(Relationships._GA_CHANGEFEED_OLDEST_CHANGE, Direction.OUTGOING);
            assertNotNull(rel);
            assertEquals(1L, rel.getEndNode().getProperty(SEQUENCE));
            tx.success();
        }
    }

    @Test
    public void graphChangesShouldAppearInTheChangeFeed() {
        Node node1, node2;
        try (Transaction tx = database.beginTx()) {
            node1 = database.createNode();
            node1.setProperty("name", "MB");
            node1.addLabel(DynamicLabel.label("Person"));
            node2 = database.createNode();
            node2.addLabel(DynamicLabel.label("Company"));
            node1.createRelationshipTo(node2, DynamicRelationshipType.withName("WORKS_AT"));

            tx.success();
        }

        try (Transaction tx = database.beginTx()) {
            node2.setProperty("name", "GraphAware");
            node2.setProperty("location", "London");
            tx.success();
        }

        try (Transaction tx = database.beginTx()) {
            node1.setProperty("name", "Michal");
            node2.removeProperty("location");
            node2.removeLabel(DynamicLabel.label("Company"));
            tx.success();
        }

        List<ChangeSet> changes = changeReader.getAllChanges();
        assertEquals(3, changes.size());

        ChangeSet set1 = changes.get(0);
        long set1Date = set1.getTimestamp();
        assertEquals(2, set1.getChanges().size());
        assertTrue(set1.getChanges().contains("Changed node (:Company {location: London, name: GraphAware}) to ({name: GraphAware})"));
        assertTrue(set1.getChanges().contains("Changed node (:Person {name: MB}) to (:Person {name: Michal})"));
        assertEquals(set1.getSequence(), 3);

        ChangeSet set2 = changes.get(1);
        long set2Date = set2.getTimestamp();
        assertEquals(1, set2.getChanges().size());
        assertTrue(set2.getChanges().contains("Changed node (:Company) to (:Company {location: London, name: GraphAware})"));
        assertEquals(2, set2.getSequence());

        ChangeSet set3 = changes.get(2);
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
        Node node1, node2;
        try (Transaction tx = database.beginTx()) {
            node1 = database.createNode();
            node1.setProperty("name", "MB");
            node1.addLabel(DynamicLabel.label("Person"));
            node2 = database.createNode();
            node2.addLabel(DynamicLabel.label("Company"));

            tx.success();
        }

        try (Transaction tx = database.beginTx()) {
            node2.setProperty("name", "GraphAware");
            node2.setProperty("location", "London");
            tx.success();
        }

        try (Transaction tx = database.beginTx()) {
            node1.setProperty("name", "Michal");
            node2.removeProperty("location");
            node2.removeLabel(DynamicLabel.label("Company"));
            tx.success();
        }

        try (Transaction tx = database.beginTx()) {
            node2.delete();
            tx.success();
        }
        List<ChangeSet> changes = changeReader.getNumberOfChanges(3);
        assertEquals(3, changes.size());

        ChangeSet set1 = changes.get(0);
        long set1Date = set1.getTimestamp();
        assertEquals(1, set1.getChanges().size());
        assertTrue(set1.getChanges().contains("Deleted node ({name: GraphAware})"));


        ChangeSet set2 = changes.get(1);
        long set2Date = set2.getTimestamp();
        assertEquals(2, set2.getChanges().size());
        assertTrue(set2.getChanges().contains("Changed node (:Company {location: London, name: GraphAware}) to ({name: GraphAware})"));
        assertTrue(set2.getChanges().contains("Changed node (:Person {name: MB}) to (:Person {name: Michal})"));


        ChangeSet set3 = changes.get(2);
        long set3Date = set3.getTimestamp();
        assertEquals(1, set3.getChanges().size());
        assertTrue(set3.getChanges().contains("Changed node (:Company) to (:Company {location: London, name: GraphAware})"));

        assertTrue(set1Date >= set2Date);
        assertTrue(set2Date >= set3Date);
    }

    @Test
    public void changesSinceShouldReturnOnlyChangesSinceTheSequenceProvided() {
        Node node1, node2;
        try (Transaction tx = database.beginTx()) {
            node1 = database.createNode();
            node1.setProperty("name", "MB");
            node1.addLabel(DynamicLabel.label("Person"));
            node2 = database.createNode();
            node2.addLabel(DynamicLabel.label("Company"));

            tx.success();
        }

        try (Transaction tx = database.beginTx()) {
            node2.setProperty("name", "GraphAware");
            node2.setProperty("location", "London");
            tx.success();
        }

        try (Transaction tx = database.beginTx()) {
            node1.setProperty("name", "Michal");
            node2.removeProperty("location");
            node2.removeLabel(DynamicLabel.label("Company"));
            tx.success();
        }

        try (Transaction tx = database.beginTx()) {
            node2.delete();
            tx.success();
        }
        List<ChangeSet> changes = changeReader.getNumberOfChanges(3);
        assertEquals(3, changes.size());

        changes = changeReader.getChangesSince(2);
        assertEquals(2, changes.size());

        ChangeSet set1 = changes.get(0);
        assertEquals(1, set1.getChanges().size());
        assertTrue(set1.getChanges().contains("Deleted node ({name: GraphAware})"));
        assertEquals(4, set1.getSequence());


        ChangeSet set2 = changes.get(1);
        assertEquals(2, set2.getChanges().size());
        assertTrue(set2.getChanges().contains("Changed node (:Company {location: London, name: GraphAware}) to ({name: GraphAware})"));
        assertTrue(set2.getChanges().contains("Changed node (:Person {name: MB}) to (:Person {name: Michal})"));
        assertEquals(3, set2.getSequence());

    }

    @Test
    public void transactionsNotCommittedShouldNotReflectInTheChangeFeed() {
        Node node1, node2;

        try (Transaction tx = database.beginTx()) {
            database.schema()
                    .constraintFor(DynamicLabel.label("Person"))
                    .assertPropertyIsUnique("name")
                    .create();
            tx.success();
        }
        try (Transaction tx = database.beginTx()) {

            node1 = database.createNode();
            node1.setProperty("name", "MB");
            node1.addLabel(DynamicLabel.label("Person"));
            node2 = database.createNode();
            node2.addLabel(DynamicLabel.label("Company"));
            node1.createRelationshipTo(node2, DynamicRelationshipType.withName("WORKS_AT"));

            tx.success();
        }

        try (Transaction tx = database.beginTx()) {
            node2.setProperty("name", "GraphAware");
            node2.setProperty("location", "London");
            tx.failure();
        }

        try (Transaction tx = database.beginTx()) {
            Node node3 = database.createNode();
            node3.setProperty("name", "MB");
            node3.addLabel(DynamicLabel.label("Person"));
            tx.success();
        } catch (Exception e) {
            //tx will fail due to unique constraint violation, swallow the exception and check the feed
        }
        try (Transaction tx = database.beginTx()) {
            node1.setProperty("name", "Michal");
            tx.success();
        }

        List<ChangeSet> changes = changeReader.getAllChanges();
        assertEquals(2, changes.size());

        ChangeSet set1 = changes.get(0);
        long set1Date = set1.getTimestamp();
        assertEquals(1, set1.getChanges().size());
        assertTrue(set1.getChanges().contains("Changed node (:Person {name: MB}) to (:Person {name: Michal})"));
        assertEquals(set1.getSequence(), 2);

        ChangeSet set2 = changes.get(1);
        long set2Date = set2.getTimestamp();
        assertEquals(3, set2.getChanges().size());
        assertTrue(set2.getChanges().contains("Created node (:Company)"));
        assertTrue(set2.getChanges().contains("Created node (:Person {name: MB})"));
        assertTrue(set2.getChanges().contains("Created relationship (:Person {name: MB})-[:WORKS_AT]->(:Company)"));
        assertEquals(1, set2.getSequence());

        assertTrue(set1Date >= set2Date);

    }

    @Test
    public void fetchingChangesFromASequenceThatHasBeenPrunedShouldReturnEverything() {
        //Create 20 changes
        for (int i = 1; i <= 20; i++) {
            try (Transaction tx = database.beginTx()) {
                Node node = database.createNode();
                node.setProperty("age", i);
                tx.success();
            }
        }
        List<ChangeSet> changes = changeReader.getAllChanges();
        assertEquals(20, changes.size());
        changes = changeReader.getChangesSince(5);
        assertEquals(15, changes.size());
        assertEquals(20, changes.get(0).getSequence());
        module.doSomeWork(new EmptyContext(), database);
        assertEquals(3, changeReader.getAllChanges().size());
        changes = changeReader.getChangesSince(5);
        assertEquals(3, changes.size());
        assertEquals(20, changes.get(0).getSequence());

    }

    @Test
    public void feedShouldBePruned() throws InterruptedException {
        //Create 10 changes
        for (int i = 1; i <= 10; i++) {
            try (Transaction tx = database.beginTx()) {
                Node node = database.createNode();
                node.setProperty("age", i);
                tx.success();
            }
        }
        //Feed should not be pruned because it has not exceeded the maxChanges by 10
        List<ChangeSet> changes = changeReader.getAllChanges();
        assertEquals(10, changes.size());
        Thread.sleep(6000);  //Wait for pruning to kick in
        assertEquals(10, changes.size());

        //Add 10 more changes
        for (int i = 1; i <= 10; i++) {
            try (Transaction tx = database.beginTx()) {
                Node node = database.createNode();
                node.setProperty("age", i);
                tx.success();
            }
        }
        Thread.sleep(6000);  //Wait for pruning to kick in
        changes = changeReader.getAllChanges();
        assertEquals(3, changes.size());
    }
}
