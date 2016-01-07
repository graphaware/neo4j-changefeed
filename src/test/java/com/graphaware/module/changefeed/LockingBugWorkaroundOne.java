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

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;

@Ignore //demonstrating one workaround to https://github.com/neo4j/neo4j/issues/6036
public class LockingBugWorkaroundOne {

    private GraphDatabaseService database;

    @Before
    public void setUp() {
        database = new TestGraphDatabaseFactory().newImpermanentDatabase(Collections.<Setting<?>, String>singletonMap(GraphDatabaseFacadeFactory.Configuration.lock_manager,"community"));
        try (Transaction tx = database.beginTx()) {
            database.createNode(DynamicLabel.label("Test"));
            tx.success();
        }
    }

    @After
    public void tearDown() {
        database.shutdown();
    }

    @Test
    public void verifyConcurrentInsertsAndFetchesFromTheLinkedList() throws InterruptedException {
        ExecutorService createExecutor = Executors.newFixedThreadPool(500);
        int noRequests = 10000;
        final AtomicBoolean success = new AtomicBoolean(true);

        for (int i = 0; i < noRequests; i++) {
            createExecutor.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        increment();
                    } catch (Exception e) {
                        success.set(false);
                        e.printStackTrace();
                    }
                }
            });
            createExecutor.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        System.out.println("counter = " + get());
                    } catch (Exception e) {
                        success.set(false);
                        e.printStackTrace();
                    }
                }
            });
        }

        long start = System.currentTimeMillis();

        createExecutor.shutdown();
        createExecutor.awaitTermination(120, TimeUnit.SECONDS);

        System.out.println("Took " + (System.currentTimeMillis() - start) + " ms");

        assertTrue("At least one operation failed", success.get());

        try (Transaction tx = database.beginTx()) {
            assertEquals(noRequests, database.getNodeById(0).getProperty("counter"));
            tx.success();
        }
    }

    private void increment() {
        try (Transaction tx = database.beginTx()) {
            Node node = database.getNodeById(0);

            tx.acquireWriteLock(node);

            node.setProperty("counter", (int) node.getProperty("counter", 0) + 1);

            tx.success();
        }
    }

    private int get() {
        int result = -1;

        try (Transaction tx = database.beginTx()) {
            Node node = database.getNodeById(0);
            tx.acquireWriteLock(node);
            result = (int) node.getProperty("counter", 0);
            tx.success();
        }

        return result;
    }
}
