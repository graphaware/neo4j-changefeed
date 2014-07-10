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
