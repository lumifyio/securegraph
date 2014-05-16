package org.securegraph.query;

import org.securegraph.*;
import org.securegraph.util.FilterIterable;

import java.util.Map;

public abstract class VertexQueryBase extends QueryBase implements VertexQuery {
    private final Vertex sourceVertex;

    protected VertexQueryBase(Graph graph, Vertex sourceVertex, String queryString, Map<String, PropertyDefinition> propertyDefinitions, Authorizations authorizations) {
        super(graph, queryString, propertyDefinitions, authorizations);
        this.sourceVertex = sourceVertex;
    }

    @Override
    public abstract Iterable<Vertex> vertices();

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

    @Override
    public Iterable<Edge> edges(Direction direction, final String label) {
        return new FilterIterable<Edge>(edges(direction)) {
            @Override
            protected boolean isIncluded(Edge o) {
                return label.equals(o.getLabel());
            }
        };
    }

    public Vertex getSourceVertex() {
        return sourceVertex;
    }
}
