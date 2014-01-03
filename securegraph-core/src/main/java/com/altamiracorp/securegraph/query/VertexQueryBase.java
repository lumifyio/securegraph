package com.altamiracorp.securegraph.query;

import com.altamiracorp.securegraph.*;
import com.altamiracorp.securegraph.util.ConvertingIterable;
import com.altamiracorp.securegraph.util.FilterIterable;

public abstract class VertexQueryBase extends QueryBase implements VertexQuery {
    private final Vertex sourceVertex;

    protected VertexQueryBase(Graph graph, Authorizations authorizations, Vertex sourceVertex) {
        super(graph, authorizations);
        this.sourceVertex = sourceVertex;
    }

    @Override
    public Iterable<Vertex> vertices() {
        return new ConvertingIterable<Edge, Vertex>(edges()) {
            @Override
            protected Vertex convert(Edge edge) {
                if (edge.getVertexId(Direction.IN).equals(sourceVertex.getId())) {
                    return edge.getVertex(Direction.OUT, getParameters().getAuthorizations());
                }
                if (edge.getVertexId(Direction.OUT).equals(sourceVertex.getId())) {
                    return edge.getVertex(Direction.IN, getParameters().getAuthorizations());
                }
                throw new IllegalStateException("Neither the in vertex or the out vertex of edge [" + edge + "] equals the source vertex.");
            }
        };
    }

    @Override
    public abstract Iterable<Edge> edges();

    @Override
    public Iterable<Edge> edges(final Direction direction) {
        return new FilterIterable<Edge>(edges()) {
            @Override
            protected boolean isIncluded(Edge src, Edge edge) {
                switch (direction) {
                    case BOTH:
                        return true;
                    case IN:
                        return edge.getVertexId(Direction.IN).equals(sourceVertex.getId());
                    case OUT:
                        return edge.getVertexId(Direction.OUT).equals(sourceVertex.getId());
                    default:
                        throw new RuntimeException("Unexpected direction: " + direction);
                }
            }
        };
    }

    public Vertex getSourceVertex() {
        return sourceVertex;
    }
}
