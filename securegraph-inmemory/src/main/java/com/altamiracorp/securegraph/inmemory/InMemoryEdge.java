package com.altamiracorp.securegraph.inmemory;

import com.altamiracorp.securegraph.*;

public class InMemoryEdge extends InMemoryElement implements Edge {
    private final Object outVertexId;
    private final Object inVertexId;
    private final String label;

    protected InMemoryEdge(Graph graph, Object edgeId, Object outVertexId, Object inVertexId, String label, Visibility visibility, Property[] properties) {
        super(graph, edgeId, visibility, properties);
        this.outVertexId = outVertexId;
        this.inVertexId = inVertexId;
        this.label = label;
    }

    @Override
    public String getLabel() {
        return this.label;
    }

    @Override
    public Object getVertexId(Direction direction) {
        switch (direction) {
            case IN:
                return inVertexId;
            case OUT:
                return outVertexId;
            default:
                throw new IllegalArgumentException("Unexpected direction: " + direction);
        }
    }

    @Override
    public Vertex getVertex(Direction direction, Authorizations authorizations) {
        return getGraph().getVertex(getVertexId(direction), authorizations);
    }
}
