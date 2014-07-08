package com.graphaware.changefeed;

import com.graphaware.test.integration.NeoServerIntegrationTest;
import junit.framework.Assert;
import org.apache.http.HttpStatus;
import org.junit.Test;

import static com.graphaware.test.util.TestUtils.executeCypher;
import static com.graphaware.test.util.TestUtils.get;


public class ChangeFeedModuleServerIntegrationTest extends NeoServerIntegrationTest {

    public ChangeFeedModuleServerIntegrationTest() {
        super("neo4j-changefeed.properties");
    }

    @Test
    public void graphChangesShouldAppearInChangeFeed() {
        executeCypher("http://localhost:7474/", "CREATE (p:Person {name: 'MB'})");
        Assert.assertTrue(get("http://localhost:7474/graphaware/changefeed", HttpStatus.SC_OK).contains("Created node (:Person {name: MB})"));


    }
}
