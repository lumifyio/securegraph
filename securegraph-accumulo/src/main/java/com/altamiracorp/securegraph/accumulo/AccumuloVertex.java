package com.altamiracorp.securegraph.accumulo;

import com.altamiracorp.securegraph.*;
import com.altamiracorp.securegraph.query.VertexQuery;
import com.altamiracorp.securegraph.util.ConvertingIterable;
import com.altamiracorp.securegraph.util.FilterIterable;
import com.altamiracorp.securegraph.util.JoinIterable;
import com.altamiracorp.securegraph.util.LookAheadIterable;
import org.apache.hadoop.io.Text;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class AccumuloVertex extends AccumuloElement implements Vertex {
    public static final Text CF_SIGNAL = new Text("V");
    public static final Text CF_OUT_EDGE = new Text("EOUT");
    public static final Text CF_IN_EDGE = new Text("EIN");
    public static final Text CF_OUT_VERTEX = new Text("VOUT");
    public static final Text CF_IN_VERTEX = new Text("VIN");
    private final Set<Object> inEdgeIds;
    private final Set<Object> outEdgeIds;
    private final Set<Object> inVertexIds;
    private final Set<Object> outVertexIds;

    AccumuloVertex(AccumuloGraph graph, Object vertexId, Visibility vertexVisibility, Iterable<Property> properties) {
        this(graph, vertexId, vertexVisibility, properties, new HashSet<Object>(), new HashSet<Object>(), new HashSet<Object>(), new HashSet<Object>());
    }

    AccumuloVertex(AccumuloGraph graph, Object vertexId, Visibility vertexVisibility, Iterable<Property> properties, Set<Object> inEdgeIds, Set<Object> outEdgeIds, Set<Object> inVertexIds, Set<Object> outVertexIds) {
        super(graph, vertexId, vertexVisibility, properties);
        this.inEdgeIds = inEdgeIds;
        this.outEdgeIds = outEdgeIds;
        this.inVertexIds = inVertexIds;
        this.outVertexIds = outVertexIds;
    }

    @Override
    public Iterable<Edge> getEdges(Direction direction, Authorizations authorizations) {
        switch (direction) {
            case BOTH:
                // TODO: Can't we concat the two id lists together and do a single scan, skipping the JoinIterable?
                return new JoinIterable<Edge>(
                        getGraph().getEdges(inEdgeIds, authorizations),
                        getGraph().getEdges(outEdgeIds, authorizations)
                );
            case IN:
                return getGraph().getEdges(inEdgeIds, authorizations);
            case OUT:
                return getGraph().getEdges(outEdgeIds, authorizations);
            default:
                throw new SecureGraphException("Unexpected direction: " + direction);
        }
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
    public Iterable<Edge> getEdges(final Vertex otherVertex, Direction direction, Authorizations authorizations) {
        return new FilterIterable<Edge>(getEdges(direction, authorizations)) {
            @Override
            protected boolean isIncluded(Edge edge) {
                return edge.getOtherVertexId(getId()).equals(otherVertex.getId());
            }
        };
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
    public Iterable<Edge> getEdges(final Vertex otherVertex, Direction direction, String[] labels, Authorizations authorizations) {
        return new FilterIterable<Edge>(getEdges(direction, labels, authorizations)) {
            @Override
            protected boolean isIncluded(Edge edge) {
                return edge.getOtherVertexId(getId()).equals(otherVertex.getId());
            }
        };
    }

    @Override
    public Iterable<Vertex> getVertices(Direction direction, final Authorizations authorizations) {
        return getGraph().getVertices(getVertexIds(direction, authorizations), authorizations);
    }

    @Override
    public Iterable<Vertex> getVertices(Direction direction, String label, Authorizations authorizations) {
        return getVertices(direction, new String[]{label}, authorizations);
    }

    @Override
    public Iterable<Vertex> getVertices(Direction direction, String[] labels, final Authorizations authorizations) {
        return new ConvertingIterable<Edge, Vertex>(getEdges(direction, labels, authorizations)) {
            @Override
            protected Vertex convert(Edge edge) {
                return edge.getOtherVertex(getId(), authorizations);
            }
        };
    }

    public Iterable<Object> getVertexIds(Direction direction, Authorizations authorizations) {
        switch (direction) {
            case BOTH:
                return new JoinIterable<Object>(inVertexIds, outVertexIds);
            case IN:
                return this.inVertexIds;
            case OUT:
                return this.outVertexIds;
            default:
                throw new SecureGraphException("Unexpected direction: " + direction);
        }
    }

    @Override
    public VertexQuery query(Authorizations authorizations) {
        return query(null, authorizations);
    }

    @Override
    public VertexQuery query(String queryString, Authorizations authorizations) {
        return getGraph().getSearchIndex().queryVertex(getGraph(), this, queryString, authorizations);
    }

    void addOutEdge(Edge edge) {
        this.outEdgeIds.add(edge.getId().toString());
        this.outVertexIds.add(edge.getVertexId(Direction.IN));
    }

    void removeOutEdge(Edge edge) {
        this.outEdgeIds.remove(edge.getId().toString());
        this.outVertexIds.remove(edge.getVertexId(Direction.IN));
    }

    void addInEdge(Edge edge) {
        this.inEdgeIds.add(edge.getId().toString());
        this.inVertexIds.add(edge.getVertexId(Direction.OUT));
    }

    void removeInEdge(Edge edge) {
        this.inEdgeIds.remove(edge.getId().toString());
        this.inVertexIds.remove(edge.getVertexId(Direction.OUT));
    }

    private static abstract class ElementsByIdsIterable<T> extends LookAheadIterable<Object, T> {
        protected final Graph graph;
        protected final Iterable<Object> idsList;
        protected final Authorizations authorizations;

        public ElementsByIdsIterable(Graph graph, Iterable<Object> idsList, Authorizations authorizations) {
            this.graph = graph;
            this.idsList = idsList;
            this.authorizations = authorizations;
        }

        @Override
        protected abstract T convert(Object edgeId);

        @Override
        protected boolean isIncluded(Object src, T dest) {
            return dest != null;
        }

        @Override
        protected Iterator<Object> createIterator() {
            return idsList.iterator();
        }
    }
}
