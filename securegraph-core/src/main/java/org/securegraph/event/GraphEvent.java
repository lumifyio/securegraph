package org.securegraph.event;

import org.securegraph.Graph;

public abstract class GraphEvent {
    private final Graph graph;
    private final Thread thread;

    protected GraphEvent(Graph graph, Thread thread) {
        this.graph = graph;
        this.thread = thread;
    }

    public Graph getGraph() {
        return graph;
    }

    public Thread getThread() {
        return thread;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof GraphEvent) {
            GraphEvent other = (GraphEvent) obj;
            return getGraph().equals(other.getGraph()) && getThread().equals(other.getThread());
        }
        return super.equals(obj);
    }
}
