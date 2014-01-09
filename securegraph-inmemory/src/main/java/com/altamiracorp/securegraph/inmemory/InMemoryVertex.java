package com.altamiracorp.securegraph.inmemory;

import com.altamiracorp.securegraph.*;
import com.altamiracorp.securegraph.query.VertexQuery;
import com.altamiracorp.securegraph.util.ConvertingIterable;
import com.altamiracorp.securegraph.util.FilterIterable;
import org.json.JSONObject;

public class InMemoryVertex extends InMemoryElement implements Vertex {
    protected InMemoryVertex(Graph graph, Object id, Visibility visibility, Property[] properties) {
        super(graph, id, visibility, properties);
    }

    @Override
    public Iterable<Edge> getEdges(final Direction direction, Authorizations authorizations) {
        return new FilterIterable<Edge>(getGraph().getEdgesFromVertex(getId(), authorizations)) {
            @Override
            protected boolean isIncluded(Edge edge) {
                switch (direction) {
                    case IN:
                        return edge.getVertexId(Direction.IN).equals(getId());
                    case OUT:
                        return edge.getVertexId(Direction.OUT).equals(getId());
                    default:
                        return true;
                }
            }
        };
    }

    @Override
    public Iterable<Edge> getEdges(Direction direction, String label, Authorizations authorizations) {
        return getEdges(direction, new String[]{label}, authorizations);
    }

    @Override
    public Iterable<Edge> getEdges(Direction direction, final String[] labels, Authorizations authorizations) {
        return new FilterIterable<Edge>(getEdges(direction, authorizations)) {
            @Override
            protected boolean isIncluded(Edge edge) {
                for (String label : labels) {
                    if (label.equals(edge.getLabel())) {
                        return true;
                    }
                }
                return false;
            }
        };
    }

    @Override
    public Iterable<Vertex> getVertices(Direction direction, final Authorizations authorizations) {
        return new ConvertingIterable<Edge, Vertex>(getEdges(direction, authorizations)) {
            @Override
            protected Vertex convert(Edge edge) {
                return getOtherVertexFromEdge(edge, authorizations);
            }
        };
    }

    @Override
    public Iterable<Vertex> getVertices(Direction direction, String label, Authorizations authorizations) {
        return getVertices(direction, new String[]{label}, authorizations);
    }

    @Override
    public Iterable<Vertex> getVertices(final Direction direction, final String[] labels, final Authorizations authorizations) {
        return new ConvertingIterable<Edge, Vertex>(getEdges(direction, labels, authorizations)) {
            @Override
            protected Vertex convert(Edge edge) {
                return getOtherVertexFromEdge(edge, authorizations);
            }
        };
    }

    private Vertex getOtherVertexFromEdge(Edge edge, Authorizations authorizations) {
        if (edge.getVertexId(Direction.IN).equals(getId())) {
            return edge.getVertex(Direction.OUT, authorizations);
        }
        if (edge.getVertexId(Direction.OUT).equals(getId())) {
            return edge.getVertex(Direction.IN, authorizations);
        }
        throw new IllegalStateException("Edge does not contain vertex on either end");
    }

    @Override
    public VertexQuery query(Authorizations authorizations) {
        return query(null, authorizations);
    }

    @Override
    public VertexQuery query(String queryString, Authorizations authorizations) {
        return getGraph().getSearchIndex().queryVertex(getGraph(), this, queryString, authorizations);
    }

    static InMemoryVertex fromJson(Graph graph, Object id, JSONObject jsonObject) {
        Visibility visibility = InMemoryElement.fromJsonVisibility(jsonObject);
        Property[] properties = InMemoryElement.fromJsonProperties(jsonObject);
        return new InMemoryVertex(graph, id, visibility, properties);
    }
}
