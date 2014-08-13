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
import com.graphaware.module.changefeed.io.ChangeReader;
import com.graphaware.runtime.ProductionRuntime;
import com.graphaware.test.integration.DatabaseIntegrationTest;
import org.junit.Test;
import org.neo4j.graphdb.*;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.util.Collection;
import java.util.Iterator;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;


public class ChangeFeedEmbeddedDeclarativeIntegrationTest extends DatabaseIntegrationTest {

    private ChangeReader changeReader;

    @Override
    protected GraphDatabaseService createDatabase() {
        return new TestGraphDatabaseFactory().newImpermanentDatabaseBuilder()
                .loadPropertiesFromFile(this.getClass().getClassLoader().getResource("neo4j-changefeed.properties").getPath())
                .newGraphDatabase();
    }

    public void setUp() throws Exception {
        super.setUp();

        ProductionRuntime.getRuntime(getDatabase()).waitUntilStarted();

        changeReader = new CachingGraphChangeReader(getDatabase());
    }

    @Test
    public void graphChangesShouldAppearInTheChangeFeed() {
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


        ChangeSet set2 = it.next();
        long set2Date = set2.getTimestamp();
        assertEquals(1, set2.getChanges().size());
        assertTrue(set2.getChanges().contains("Changed node (:Company) to (:Company {location: London, name: GraphAware})"));

        ChangeSet set3 = it.next();
        long set3Date = set3.getTimestamp();
        assertEquals(3, set3.getChanges().size());
        assertTrue(set3.getChanges().contains("Created node (:Company)"));
        assertTrue(set3.getChanges().contains("Created node (:Person {name: MB})"));
        assertTrue(set3.getChanges().contains("Created relationship (:Person {name: MB})-[:WORKS_AT]->(:Company)"));

        assertTrue(set1Date >= set2Date);
        assertTrue(set2Date >= set3Date);

    }

}
