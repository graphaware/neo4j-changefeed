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

public class ChangeFeedFactory {
    private static ChangeSetQueue ourInstance = null;

    public static ChangeSetQueue getInstance() {
        if (ourInstance == null) {
            throw new IllegalStateException("ChangeFeedFactory has not been initialized");
        }
        return ourInstance;
    }

    public static void initialize(int maxChanges) {
        ourInstance = new ChangeSetQueue(maxChanges);
    }

    private ChangeFeedFactory() {
    }
}
