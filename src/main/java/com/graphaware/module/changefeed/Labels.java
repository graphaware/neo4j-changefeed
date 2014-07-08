package com.graphaware.module.changefeed;

import org.neo4j.graphdb.Label;

/**
 * Labels used by the ChangeFeed module
 */
public enum Labels implements Label {
    ChangeFeed,
    ChangeSet
}
