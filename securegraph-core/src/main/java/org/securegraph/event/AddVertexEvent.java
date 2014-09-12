package org.securegraph.event;

import org.securegraph.Graph;
import org.securegraph.Vertex;

public class AddVertexEvent extends GraphEvent {
    private final Vertex vertex;

    public AddVertexEvent(Graph graph, Thread thread, Vertex vertex) {
        super(graph, thread);
        this.vertex = vertex;
    }

    public Vertex getVertex() {
        return vertex;
    }

    @Override
    public String toString() {
        return "AddVertexEvent{vertex=" + vertex + '}';
    }

    @Override
    public int hashCode() {
        return this.vertex.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof AddVertexEvent) {
            AddVertexEvent other = (AddVertexEvent) obj;
            if (!getVertex().equals(other.getVertex())) {
                return false;
            }
        }
        return super.equals(obj);
    }
}
