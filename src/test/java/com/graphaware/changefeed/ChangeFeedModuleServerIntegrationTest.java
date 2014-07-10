package com.graphaware.changefeed;

import com.graphaware.test.integration.NeoServerIntegrationTest;
import junit.framework.Assert;
import org.apache.http.HttpStatus;
import org.junit.Test;

import static com.graphaware.test.util.TestUtils.executeCypher;
import static com.graphaware.test.util.TestUtils.get;


public class ChangeFeedModuleServerIntegrationTest extends NeoServerIntegrationTest {

    /**
     * {@inheritDoc}
     */
    @Override
    protected String neo4jConfigFile() {
        return "neo4j-changefeed.properties";
    }

    @Test
    public void graphChangesShouldAppearInChangeFeed() {
        executeCypher(baseUrl(), "CREATE (p:Person {name: 'MB'})");
        Assert.assertTrue(get(baseUrl() + "/graphaware/changefeed", HttpStatus.SC_OK).contains("Created node (:Person {name: MB})"));


    }
}
