package com.altamiracorp.securegraph.inmemory;

import com.altamiracorp.securegraph.*;
import com.altamiracorp.securegraph.inmemory.util.EdgeToEdgeIdIterable;
import com.altamiracorp.securegraph.mutation.ExistingElementMutation;
import com.altamiracorp.securegraph.mutation.ExistingElementMutationImpl;
import com.altamiracorp.securegraph.query.VertexQuery;
import com.altamiracorp.securegraph.util.ConvertingIterable;
import com.altamiracorp.securegraph.util.FilterIterable;

public class InMemoryVertex extends InMemoryElement implements Vertex {
    protected InMemoryVertex(Graph graph, Object id, Visibility visibility, Iterable<Property> properties) {
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
    public Iterable<Object> getEdgeIds(Direction direction, Authorizations authorizations) {
        return new EdgeToEdgeIdIterable(getEdges(direction, authorizations));
    }

    @Override
    public Iterable<Edge> getEdges(Direction direction, String label, Authorizations authorizations) {
        return getEdges(direction, new String[]{label}, authorizations);
    }

    @Override
    public Iterable<Object> getEdgeIds(Direction direction, String label, Authorizations authorizations) {
        return new EdgeToEdgeIdIterable(getEdges(direction, label, authorizations));
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
    public Iterable<Object> getEdgeIds(Direction direction, String[] labels, Authorizations authorizations) {
        return new EdgeToEdgeIdIterable(getEdges(direction, labels, authorizations));
    }

    @Override
    public Iterable<Edge> getEdges(final Vertex otherVertex, Direction direction, Authorizations authorizations) {
        return new FilterIterable<Edge>(getEdges(direction, authorizations)) {
            @Override
            protected boolean isIncluded(Edge edge) {
                return edge.getOtherVertexId(getId()).equals(otherVertex.getId());
            }
        };
    }

    @Override
    public Iterable<Object> getEdgeIds(Vertex otherVertex, Direction direction, Authorizations authorizations) {
        return new EdgeToEdgeIdIterable(getEdges(otherVertex, direction, authorizations));
    }

    @Override
    public Iterable<Edge> getEdges(final Vertex otherVertex, Direction direction, String label, Authorizations authorizations) {
        return new FilterIterable<Edge>(getEdges(direction, label, authorizations)) {
            @Override
            protected boolean isIncluded(Edge edge) {
                return edge.getOtherVertexId(getId()).equals(otherVertex.getId());
            }
        };
    }

    @Override
    public Iterable<Object> getEdgeIds(Vertex otherVertex, Direction direction, String label, Authorizations authorizations) {
        return new EdgeToEdgeIdIterable(getEdges(otherVertex, direction, label, authorizations));
    }

    @Override
    public Iterable<Edge> getEdges(final Vertex otherVertex, Direction direction, String[] labels, Authorizations authorizations) {
        return new FilterIterable<Edge>(getEdges(direction, labels, authorizations)) {
            @Override
            protected boolean isIncluded(Edge edge) {
                return edge.getOtherVertexId(getId()).equals(otherVertex.getId());
            }
        };
    }

    @Override
    public Iterable<Object> getEdgeIds(Vertex otherVertex, Direction direction, String[] labels, Authorizations authorizations) {
        return new EdgeToEdgeIdIterable(getEdges(otherVertex, direction, labels, authorizations));
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

    @Override
    public Iterable<Object> getVertexIds(Direction direction, String label, Authorizations authorizations) {
        return new ConvertingIterable<Vertex, Object>(getVertices(direction, label, authorizations)) {
            @Override
            protected Object convert(Vertex o) {
                return o.getId();
            }
        };
    }

    @Override
    public Iterable<Object> getVertexIds(Direction direction, String[] labels, Authorizations authorizations) {
        return new ConvertingIterable<Vertex, Object>(getVertices(direction, labels, authorizations)) {
            @Override
            protected Object convert(Vertex o) {
                return o.getId();
            }
        };
    }

    @Override
    public Iterable<Object> getVertexIds(Direction direction, Authorizations authorizations) {
        return new ConvertingIterable<Vertex, Object>(getVertices(direction, authorizations)) {
            @Override
            protected Object convert(Vertex o) {
                return o.getId();
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

    @Override
    public ExistingElementMutation<Vertex> prepareMutation() {
        return new ExistingElementMutationImpl<Vertex>(this) {
            @Override
            public Vertex save() {
                saveExistingElementMutation(this);
                return getElement();
            }
        };
    }
}
