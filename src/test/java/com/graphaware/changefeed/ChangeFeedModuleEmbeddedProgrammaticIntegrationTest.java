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

package com.graphaware.changefeed;

import com.graphaware.module.changefeed.*;
import com.graphaware.runtime.GraphAwareRuntime;
import com.graphaware.runtime.GraphAwareRuntimeFactory;
import junit.framework.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.*;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.graphaware.common.util.IterableUtils.getSingleOrNull;
import static org.neo4j.tooling.GlobalGraphOperations.at;

/**
 * Tests the module in an embedded db programmatically
 */
public class ChangeFeedModuleEmbeddedProgrammaticIntegrationTest {

    private GraphDatabaseService database;
    private ChangeFeed changeFeed;


    @Before
    public void setUp() {
        database = new TestGraphDatabaseFactory().newImpermanentDatabase();
        GraphAwareRuntime runtime = GraphAwareRuntimeFactory.createRuntime(database);
        Map<String, String> config = new HashMap<>();
        config.put("maxChanges", "3");

        runtime.registerModule(new ChangeFeedModule("CFM", database, config));
        changeFeed = new ChangeFeed(database);
    }

    @After
    public void tearDown() {
        database.shutdown();
    }

    @Test
    public void feedShouldBeEmptyOnANewDatabase() {
        List<ChangeSet> changes = changeFeed.getChanges();
        Assert.assertTrue(changes.size() == 0);
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
            Node changeRoot = getSingleOrNull(at(database).getAllNodesWithLabel(Labels.ChangeFeed));
            Assert.assertNotNull(changeRoot);
            Relationship rel = changeRoot.getSingleRelationship(Relationships.OLDEST_CHANGE, Direction.OUTGOING);
            Assert.assertNotNull(rel);
            Assert.assertEquals(1, rel.getEndNode().getProperty("sequence"));
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

        List<ChangeSet> changes = changeFeed.getChanges();
        Assert.assertTrue(changes.size() == 3);

        ChangeSet set1 = changes.get(0);
        Date set1Date = set1.getChangeDate();
        Assert.assertTrue(set1.getChanges().size() == 2);
        Assert.assertTrue(set1.getChanges().contains("Changed node (:Company {location: London, name: GraphAware}) to ({name: GraphAware})"));
        Assert.assertTrue(set1.getChanges().contains("Changed node (:Person {name: MB}) to (:Person {name: Michal})"));
        Assert.assertEquals(set1.getSequence(), 3);

        ChangeSet set2 = changes.get(1);
        Date set2Date = set2.getChangeDate();
        Assert.assertTrue(set2.getChanges().size() == 1);
        Assert.assertTrue(set2.getChanges().contains("Changed node (:Company) to (:Company {location: London, name: GraphAware})"));
        Assert.assertEquals(2, set2.getSequence());

        ChangeSet set3 = changes.get(2);
        Date set3Date = set3.getChangeDate();
        Assert.assertTrue(set3.getChanges().size() == 3);
        Assert.assertTrue(set3.getChanges().contains("Created node (:Company)"));
        Assert.assertTrue(set3.getChanges().contains("Created node (:Person {name: MB})"));
        Assert.assertTrue(set3.getChanges().contains("Created relationship (:Person {name: MB})-[:WORKS_AT]->(:Company)"));
        Assert.assertEquals(1, set3.getSequence());

        Assert.assertTrue(set1Date.getTime() >= set2Date.getTime());
        Assert.assertTrue(set2Date.getTime() >= set3Date.getTime());

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
        List<ChangeSet> changes = changeFeed.getChanges();
        Assert.assertTrue(changes.size() == 3);

        ChangeSet set1 = changes.get(0);
        Date set1Date = set1.getChangeDate();
        Assert.assertTrue(set1.getChanges().size() == 1);
        Assert.assertTrue(set1.getChanges().contains("Deleted node ({name: GraphAware})"));


        ChangeSet set2 = changes.get(1);
        Date set2Date = set2.getChangeDate();
        Assert.assertTrue(set2.getChanges().size() == 2);
        Assert.assertTrue(set2.getChanges().contains("Changed node (:Company {location: London, name: GraphAware}) to ({name: GraphAware})"));
        Assert.assertTrue(set2.getChanges().contains("Changed node (:Person {name: MB}) to (:Person {name: Michal})"));


        ChangeSet set3 = changes.get(2);
        Date set3Date = set3.getChangeDate();
        Assert.assertTrue(set3.getChanges().size() == 1);
        Assert.assertTrue(set3.getChanges().contains("Changed node (:Company) to (:Company {location: London, name: GraphAware})"));

        Assert.assertTrue(set1Date.getTime() >= set2Date.getTime());
        Assert.assertTrue(set2Date.getTime() >= set3Date.getTime());
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
        List<ChangeSet> changes = changeFeed.getChanges();
        Assert.assertTrue(changes.size() == 3);

        changes = changeFeed.getChanges(2);
        Assert.assertTrue(changes.size() == 2);

        ChangeSet set1 = changes.get(0);
        Assert.assertTrue(set1.getChanges().size() == 1);
        Assert.assertTrue(set1.getChanges().contains("Deleted node ({name: GraphAware})"));
        Assert.assertEquals(4, set1.getSequence());


        ChangeSet set2 = changes.get(1);
        Assert.assertTrue(set2.getChanges().size() == 2);
        Assert.assertTrue(set2.getChanges().contains("Changed node (:Company {location: London, name: GraphAware}) to ({name: GraphAware})"));
        Assert.assertTrue(set2.getChanges().contains("Changed node (:Person {name: MB}) to (:Person {name: Michal})"));
        Assert.assertEquals(3, set2.getSequence());

    }

    @Test
    public void transactionsNotCommittedShouldNotReflectInTheChangeFeed() {
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
            tx.failure();
        }


        try (Transaction tx = database.beginTx()) {
            node1.setProperty("name", "Michal");
            tx.success();
        }

        List<ChangeSet> changes = changeFeed.getChanges();
        Assert.assertTrue(changes.size() == 2);

        ChangeSet set1 = changes.get(0);
        Date set1Date = set1.getChangeDate();
        Assert.assertTrue(set1.getChanges().size() == 1);
        Assert.assertTrue(set1.getChanges().contains("Changed node (:Person {name: MB}) to (:Person {name: Michal})"));
        Assert.assertEquals(set1.getSequence(), 2);

        ChangeSet set2 = changes.get(1);
        Date set2Date = set2.getChangeDate();
        Assert.assertTrue(set2.getChanges().size() == 3);
        Assert.assertTrue(set2.getChanges().contains("Created node (:Company)"));
        Assert.assertTrue(set2.getChanges().contains("Created node (:Person {name: MB})"));
        Assert.assertTrue(set2.getChanges().contains("Created relationship (:Person {name: MB})-[:WORKS_AT]->(:Company)"));
        Assert.assertEquals(1, set2.getSequence());

        Assert.assertTrue(set1Date.getTime() >= set2Date.getTime());

    }

}
