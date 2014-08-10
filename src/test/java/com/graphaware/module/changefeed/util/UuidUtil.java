package com.graphaware.module.changefeed.util;

import com.graphaware.module.changefeed.domain.Labels;
import com.graphaware.module.changefeed.domain.Relationships;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

import static com.graphaware.common.util.IterableUtils.getSingleOrNull;
import static com.graphaware.module.changefeed.domain.Properties.MODULE_ID;
import static com.graphaware.module.changefeed.domain.Properties.UUID;

/**
 * Created by luanne on 09/08/14.
 */
public class UuidUtil {

    public static String getUuidOfLatestChange(GraphDatabaseService database) {
        String uuid;

        try (Transaction tx = database.beginTx()) {
            Node root = getSingleOrNull(database.findNodesByLabelAndProperty(Labels._GA_ChangeFeed, MODULE_ID, "CFM"));
            if(root==null) {
                throw new IllegalStateException("The ChangeFeed node should have been created");
            }
            Node latestChangeNode =  root.getSingleRelationship(Relationships._GA_CHANGEFEED_NEXT_CHANGE, Direction.OUTGOING).getEndNode();
            uuid=(String) latestChangeNode.getProperty(UUID);
            tx.success();
        }
        return uuid;
    }
}
