package com.graphaware.module.changefeed;

import org.neo4j.graphdb.RelationshipType;

/**
 * Created by luanne on 05/07/14.
 */
public enum Relationships implements RelationshipType {
    NEXT,
    OLDEST_CHANGE
}
