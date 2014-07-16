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

import org.junit.Test;

import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static junit.framework.Assert.assertEquals;


public class ChangeSetQueueTest {

    @Test
    public void capacityShouldNotBeExceeded() {
        ChangeSetQueue queue = new ChangeSetQueue(3);

        ChangeSet c1 = new ChangeSet(1);
        ChangeSet c2 = new ChangeSet(2);
        ChangeSet c3 = new ChangeSet(3);
        ChangeSet c4 = new ChangeSet(4);

        queue.add(c1);
        queue.add(c2);
        queue.add(c3);
        queue.add(c4);

        assertEquals(3, queue.getAllChanges().size());
        Iterator<ChangeSet> it = queue.getAllChanges().iterator();
        assertEquals(4, it.next().getSequence());
        assertEquals(3, it.next().getSequence());
        assertEquals(2, it.next().getSequence());

    }

    @Test
    public void changesReturnedShouldNotExceedLimit() {
        ChangeSetQueue queue = new ChangeSetQueue(3);

        ChangeSet c1 = new ChangeSet(1);
        ChangeSet c2 = new ChangeSet(2);
        ChangeSet c3 = new ChangeSet(3);
        ChangeSet c4 = new ChangeSet(4);

        queue.add(c1);
        queue.add(c2);
        queue.add(c3);
        queue.add(c4);

        assertEquals(3, queue.getAllChanges().size());
        Collection<ChangeSet> changes = queue.getLimitedChanges(2);
        assertEquals(2, changes.size());
        Iterator<ChangeSet> it = queue.getAllChanges().iterator();
        assertEquals(4, it.next().getSequence());
        assertEquals(3, it.next().getSequence());
    }

    @Test
    public void survivesHeavyConcurrency() throws InterruptedException {
        final ChangeSetQueue queue = new ChangeSetQueue(10);
        final AtomicLong sequence = new AtomicLong(0);

        ExecutorService executor = Executors.newFixedThreadPool(100);
        for (int i = 0; i < 1000; i++) {
            executor.submit(new Runnable() {
                @Override
                public void run() {
                    queue.add(new ChangeSet(sequence.incrementAndGet()));
                }
            });
        }
        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.MINUTES);

        assertEquals(10, queue.getAllChanges().size());
    }
}
