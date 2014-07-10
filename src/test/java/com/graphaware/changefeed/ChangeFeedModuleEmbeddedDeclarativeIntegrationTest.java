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

import com.graphaware.module.changefeed.ChangeFeed;
import com.graphaware.module.changefeed.ChangeSet;
import junit.framework.Assert;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.*;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.util.Date;
import java.util.List;


public class ChangeFeedModuleEmbeddedDeclarativeIntegrationTest {

    private GraphDatabaseService database;
    private ChangeFeed changeFeed;

    @Before
    public void setUp() {
        database = new TestGraphDatabaseFactory().newImpermanentDatabaseBuilder()
                .loadPropertiesFromFile(this.getClass().getClassLoader().getResource("neo4j-changefeed.properties").getPath())
                .newGraphDatabase();
        changeFeed = new ChangeFeed(database);
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
        Assert.assertEquals(2,set1.getChanges().size());
        Assert.assertTrue(set1.getChanges().contains("Changed node (:Company {location: London, name: GraphAware}) to ({name: GraphAware})"));
        Assert.assertTrue(set1.getChanges().contains("Changed node (:Person {name: MB}) to (:Person {name: Michal})"));


        ChangeSet set2 = changes.get(1);
        Date set2Date = set2.getChangeDate();
        Assert.assertEquals(1,set2.getChanges().size());
        Assert.assertTrue(set2.getChanges().contains("Changed node (:Company) to (:Company {location: London, name: GraphAware})"));

        ChangeSet set3 = changes.get(2);
        Date set3Date = set3.getChangeDate();
        Assert.assertEquals(3,set3.getChanges().size());
        Assert.assertTrue(set3.getChanges().contains("Created node (:Company)"));
        Assert.assertTrue(set3.getChanges().contains("Created node (:Person {name: MB})"));
        Assert.assertTrue(set3.getChanges().contains("Created relationship (:Person {name: MB})-[:WORKS_AT]->(:Company)"));

        Assert.assertTrue(set1Date.getTime() >= set2Date.getTime());
        Assert.assertTrue(set2Date.getTime() >= set3Date.getTime());

    }

}
