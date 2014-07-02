package com.graphaware.changefeed;

import com.graphaware.module.changefeed.ChangeFeedModule;
import com.graphaware.module.changefeed.ChangeSet;
import junit.framework.Assert;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.*;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.util.Date;
import java.util.Deque;

/**
 * Created by luanne on 02/07/14.
 */
public class ChangeFeedModuleEmbeddedDeclarativeIntegrationTest {

    private GraphDatabaseService database;

    @Before
    public void setUp() {
        database = new TestGraphDatabaseFactory().newImpermanentDatabaseBuilder()
                .loadPropertiesFromFile(this.getClass().getClassLoader().getResource("neo4j-changefeed.properties").getPath())
                .newGraphDatabase();
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

        Deque<ChangeSet> changes = ChangeFeedModule.getChanges();
        Assert.assertTrue(changes.size() == 3);

        ChangeSet set1 = changes.removeFirst();
        Date set1Date = set1.getChangeDate();
        Assert.assertTrue(set1.getChanges().size()==2);
        Assert.assertTrue(set1.getChanges().contains("Changed node (:Company {location: London, name: GraphAware}) to ({name: GraphAware})"));
        Assert.assertTrue(set1.getChanges().contains("Changed node (:Person {name: MB}) to (:Person {name: Michal})"));


        ChangeSet set2 = changes.removeFirst();
        Date set2Date = set2.getChangeDate();
        Assert.assertTrue(set2.getChanges().size()==1);
        Assert.assertTrue(set2.getChanges().contains("Changed node (:Company) to (:Company {location: London, name: GraphAware})"));

        ChangeSet set3 = changes.removeFirst();
        Date set3Date = set3.getChangeDate();
        Assert.assertTrue(set3.getChanges().size() == 3);
        Assert.assertTrue(set3.getChanges().contains("Created node (:Company)"));
        Assert.assertTrue(set3.getChanges().contains("Created node (:Person {name: MB})"));
        Assert.assertTrue(set3.getChanges().contains("Created relationship (:Person {name: MB})-[:WORKS_AT]->(:Company)"));

        Assert.assertTrue(set1Date.getTime() >= set2Date.getTime());
        Assert.assertTrue(set2Date.getTime() >= set3Date.getTime());

    }

}
