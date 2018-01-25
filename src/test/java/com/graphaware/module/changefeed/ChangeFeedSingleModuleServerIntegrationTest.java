/*
 * Copyright (c) 2013-2016 GraphAware
 *
 * This file is part of the GraphAware Framework.
 *
 * GraphAware Framework is free software: you can redistribute it and/or modify it under the terms of
 * the GNU General Public License as published by the Free Software Foundation, either
 * version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU General Public License for more details. You should have received a copy of
 * the GNU General Public License along with this program.  If not, see
 * <http://www.gnu.org/licenses/>.
 */

package com.graphaware.module.changefeed;

import com.graphaware.test.integration.GraphAwareIntegrationTest;
import org.apache.http.HttpStatus;
import org.junit.Ignore;
import org.junit.Test;

import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static junit.framework.Assert.assertTrue;


public class ChangeFeedSingleModuleServerIntegrationTest extends GraphAwareIntegrationTest {

    @Override
    protected String configFile() {
        return "neo4j-changefeed.properties";
    }

    @Test
    public void graphChangesShouldAppearInChangeFeed() {
        httpClient.executeCypher(baseNeoUrl(), "CREATE (p:Person {name: 'MB'})");
        assertTrue(httpClient.get(baseUrl() + "/changefeed/CFM", HttpStatus.SC_OK).contains("Created node (:Person {name: MB})"));
    }

    @Test
    @Ignore //something on the way is making this 500, to be investigated
    public void unknownModuleShouldReturn404() {
        httpClient.get(baseUrl() + "/changefeed/unknown", HttpStatus.SC_NOT_FOUND);
    }

    @Test
    @Ignore("just to have a look at http://localhost:7575")
    public void test() throws InterruptedException {
        ExecutorService executorService = Executors.newFixedThreadPool(10);

        for (int i = 0; i < 100; i++) {
            executorService.submit(new Runnable() {
                @Override
                public void run() {
                    httpClient.executeCypher(baseNeoUrl(), "CREATE (p:Person {name: 'Person" + UUID.randomUUID() + "'})");
                }
            });
        }

        Thread.sleep(10000000);
    }

    @Test
    public void shouldNotBeAbleToDeleteRoot() {
        httpClient.executeCypher(baseNeoUrl(), "CREATE (michal:Person {name:'Michal'})");
        httpClient.executeCypher(baseNeoUrl(), "CREATE (luanne:Person {name:'Luanne'})");
        httpClient.executeCypher(baseNeoUrl(), "CREATE (ga:Company {name:'GraphAware'})");
        httpClient.executeCypher(baseNeoUrl(), "MATCH (michal:Person {name:'Michal'}), (ga:Company {name:'GraphAware'}), (luanne:Person {name:'Luanne'}) " +
                "MERGE (michal)-[:WORKS_FOR]->(ga)<-[:WORKS_FOR]-(luanne)");
        httpClient.executeCypher(baseNeoUrl(), "MATCH (n)-[r]-(m) DELETE n,r,m");

        httpClient.get(baseUrl() + "/changefeed/CFM", HttpStatus.SC_OK);
    }
}
