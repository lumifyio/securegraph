package com.altamiracorp.securegraph.query;

import com.altamiracorp.securegraph.*;
import com.altamiracorp.securegraph.util.ConvertingIterable;
import com.altamiracorp.securegraph.util.FilterIterable;

public abstract class VertexQueryBase extends QueryBase implements VertexQuery {
    private final Vertex sourceVertex;

    protected VertexQueryBase(Graph graph, Vertex sourceVertex, String queryString, Authorizations authorizations) {
        super(graph, queryString, authorizations);
        this.sourceVertex = sourceVertex;
    }

    @Override
    public Iterable<Vertex> vertices() {
        return new ConvertingIterable<Edge, Vertex>(edges()) {
            @Override
            protected Vertex convert(Edge edge) {
                return edge.getOtherVertex(sourceVertex.getId(), getParameters().getAuthorizations());
            }
        };
    }

    @Override
    public abstract Iterable<Edge> edges();

    @Override
    public Iterable<Edge> edges(final Direction direction) {
        return new FilterIterable<Edge>(edges()) {
            @Override
            protected boolean isIncluded(Edge edge) {
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
