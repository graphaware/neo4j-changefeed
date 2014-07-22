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

import com.graphaware.test.integration.NeoServerIntegrationTest;
import org.apache.http.HttpStatus;
import org.junit.Ignore;
import org.junit.Test;

import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.graphaware.test.util.TestUtils.executeCypher;
import static com.graphaware.test.util.TestUtils.get;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;


public class ChangeFeedMultiModuleServerIntegrationTest extends NeoServerIntegrationTest {

    /**
     * {@inheritDoc}
     */
    @Override
    protected String neo4jConfigFile() {
        return "neo4j-multi-changefeed.properties";
    }

    @Test
    public void graphChangesShouldAppearInChangeFeed() {
        executeCypher(baseUrl(), "CREATE (p:Person {name: 'MB'})");
        executeCypher(baseUrl(), "CREATE (p:Person {name: 'LM'})");
        executeCypher(baseUrl(), "CREATE (p:Person {name: 'DM'})");
        executeCypher(baseUrl(), "CREATE (p:Person {name: 'VB'})");

        assertFalse(get(baseUrl() + "/graphaware/changefeed/changefeed1/", HttpStatus.SC_OK).contains("Created node (:Person {name: MB})"));
        assertTrue(get(baseUrl() + "/graphaware/changefeed/changefeed1/", HttpStatus.SC_OK).contains("Created node (:Person {name: LM})"));
        assertTrue(get(baseUrl() + "/graphaware/changefeed/changefeed2/", HttpStatus.SC_OK).contains("Created node (:Person {name: MB})"));
        assertTrue(get(baseUrl() + "/graphaware/changefeed/changefeed2/", HttpStatus.SC_OK).contains("Created node (:Person {name: LM})"));
    }

    @Test
    public void unknownModuleShouldReturn404() {
        get(baseUrl() + "/graphaware/changefeed/CFM/", HttpStatus.SC_NOT_FOUND);
    }

    @Test
    @Ignore("just to have a look at http://localhost:7575")
    public void test() throws InterruptedException {
        ExecutorService executorService = Executors.newFixedThreadPool(10);

        for (int i = 0; i < 100; i++) {
            executorService.submit(new Runnable() {
                @Override
                public void run() {
                    executeCypher(baseUrl(), "CREATE (p:Person {name: 'Person" + UUID.randomUUID() + "'})");
                }
            });
        }

        Thread.sleep(10000000);
    }
}
