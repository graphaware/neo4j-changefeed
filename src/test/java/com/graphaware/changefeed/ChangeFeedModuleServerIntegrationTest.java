package com.graphaware.changefeed;

import com.graphaware.test.integration.NeoServerIntegrationTest;
import org.apache.http.HttpStatus;
import org.junit.Test;

import static com.graphaware.test.util.TestUtils.executeCypher;
import static com.graphaware.test.util.TestUtils.get;
import static org.junit.Assert.assertEquals;


public class ChangeFeedModuleServerIntegrationTest extends NeoServerIntegrationTest {

    public ChangeFeedModuleServerIntegrationTest() {
        super("neo4j-changefeed.properties");
    }

    @Test
    public void totalChangesOnEmptyDatabaseShouldBeZero() {
        assertEquals("[]", get("http://localhost:7474/graphaware/changefeed", HttpStatus.SC_OK));
    }

    @Test
    public void graphChangesShouldAppearInChangeFeed() {
        executeCypher("http://localhost:7474/", "CREATE (p:Person {name: 'MB'})");
        assertEquals("Created node (:Person {name: MB})", get("http://localhost:7474/graphaware/changefeed", HttpStatus.SC_OK));


    }
}
