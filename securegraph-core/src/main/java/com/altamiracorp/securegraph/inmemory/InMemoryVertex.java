package com.altamiracorp.securegraph.inmemory;

import com.altamiracorp.securegraph.*;
import com.altamiracorp.securegraph.query.VertexQuery;
import com.altamiracorp.securegraph.util.ConvertingIterable;

public class InMemoryVertex extends InMemoryElement implements Vertex {
    protected InMemoryVertex(Graph graph, Object id, Visibility visibility, Property[] properties) {
        super(graph, id, visibility, properties);
    }

    @Override
    public Iterable<Edge> getEdges(Direction direction, Authorizations authorizations) {
        return getGraph().getEdgesFromVertex(getId(), authorizations);
    }

    @Override
    public Iterable<Vertex> getVertices(Direction direction, final Authorizations authorizations) {
        return new ConvertingIterable<Edge, Vertex>(getEdges(direction, authorizations)) {
            @Override
            protected Vertex convert(Edge edge) {
                if (edge.getVertexId(Direction.IN).equals(getId())) {
                    return edge.getVertex(Direction.OUT, authorizations);
                }
                if (edge.getVertexId(Direction.OUT).equals(getId())) {
                    return edge.getVertex(Direction.IN, authorizations);
                }
                throw new IllegalStateException("Edge does not contain vertex on either end");
            }
        };
    }

    @Override
    public VertexQuery query(Authorizations authorizations) {
        return getGraph().getSearchIndex().queryVertex(getGraph(), this, authorizations);
    }
}
