package com.graphaware.module.changefeed;

import org.neo4j.graphdb.RelationshipType;

/**
 * Relationships used by the ChangeFeed module
 */
public enum Relationships implements RelationshipType {
    NEXT,
    OLDEST_CHANGE
}
