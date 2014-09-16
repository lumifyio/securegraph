package org.securegraph.event;

import org.securegraph.Graph;

public abstract class GraphEventListener {
    public abstract void onGraphEvent(GraphEvent graphEvent);
}
