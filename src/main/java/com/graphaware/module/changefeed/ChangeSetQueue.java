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

import java.util.*;
import java.util.concurrent.ConcurrentLinkedDeque;


public class ChangeSetQueue {

    private Deque<ChangeSet> changes;
    private int maxCapacity = 100;

    ChangeSetQueue(int maxCapacity) {
        changes = new ConcurrentLinkedDeque<>();
        this.maxCapacity = maxCapacity;
    }

    void add(ChangeSet changeSet) {
        while (changes.size() >= maxCapacity) {
            changes.removeLast();
        }
        changes.addFirst(changeSet);
    }

    void addAll(Collection<ChangeSet> changeSets) {
        changes.addAll(changeSets);
    }

    public Collection<ChangeSet> getAllChanges() {
        return changes;
    }

    public Collection<ChangeSet> getLimitedChanges(int limit) {
        List<ChangeSet> limitedChanges = new ArrayList<>(limit);
        if (limit >= changes.size()) {
            return changes;
        }
        int count = 0;
        for (Iterator<ChangeSet> iterator = changes.iterator(); iterator.hasNext() && count < limit; ) {
            limitedChanges.add(iterator.next());
            count++;
        }
        return limitedChanges;
    }

    public Collection<ChangeSet> getChangesSince(long sequence) {
        return getChangesSince(sequence, maxCapacity);
    }

    public Collection<ChangeSet> getChangesSince(long sequence, int limit) {
        Deque<ChangeSet> limitedChanges = new ArrayDeque<>(limit);
        if (limit >= changes.size() && changes.getFirst().getSequence() <= sequence) {
            return changes;
        }
        int count = 0;
        boolean foundStart = false;
        for (Iterator<ChangeSet> iterator = changes.descendingIterator(); iterator.hasNext() && count < limit; ) {
            ChangeSet changeSet = iterator.next();

            if (foundStart) {  //If we have found the sequence to start with, skip the comparison and just add it to the results
                limitedChanges.addFirst(changeSet);
                count++;
            } else {
                if (changeSet.getSequence() > sequence) {
                    foundStart = true;
                    limitedChanges.addFirst(changeSet);
                    count++;
                }
            }
        }
        return limitedChanges;
    }


}
