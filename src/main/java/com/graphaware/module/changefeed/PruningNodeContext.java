package com.graphaware.module.changefeed;

import com.graphaware.runtime.metadata.NodeBasedContext;
import org.neo4j.graphdb.Node;

/**
 * NodeBasedContext for the Timer Driven Module {@link com.graphaware.module.changefeed.ChangeFeedModule}
 */
public class PruningNodeContext extends NodeBasedContext {
    //TODO drop this when the timer module work is not invoked until the previous unit of work is complete
    private State state;

    public PruningNodeContext(Node node) {
        super(node);
        this.state=State.NOT_RUNNING;
    }

    public State getState() {
        return state;
    }

    public void setState(State state) {
        this.state = state;
    }

    enum State {

        NOT_RUNNING,
        INIT,
        RUNNING
    }

}
