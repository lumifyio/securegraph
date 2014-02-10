package com.altamiracorp.securegraph.accumulo;

import com.altamiracorp.securegraph.*;
import com.altamiracorp.securegraph.query.VertexQuery;
import com.altamiracorp.securegraph.util.ConvertingIterable;
import com.altamiracorp.securegraph.util.FilterIterable;
import com.altamiracorp.securegraph.util.JoinIterable;
import com.altamiracorp.securegraph.util.LookAheadIterable;
import org.apache.hadoop.io.Text;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class AccumuloVertex extends AccumuloElement implements Vertex {
    public static final Text CF_SIGNAL = new Text("V");
    public static final Text CF_OUT_EDGE = new Text("EOUT");
    public static final Text CF_IN_EDGE = new Text("EIN");
    private final Map<Object, EdgeInfo> inEdges;
    private final Map<Object, EdgeInfo> outEdges;

    public AccumuloVertex(AccumuloGraph graph, Object vertexId, Visibility vertexVisibility, Iterable<Property> properties) {
        this(graph, vertexId, vertexVisibility, properties, new HashMap<Object, EdgeInfo>(), new HashMap<Object, EdgeInfo>());
    }

    AccumuloVertex(AccumuloGraph graph, Object vertexId, Visibility vertexVisibility, Iterable<Property> properties, Map<Object, EdgeInfo> inEdges, Map<Object, EdgeInfo> outEdges) {
        super(graph, vertexId, vertexVisibility, properties);
        this.inEdges = inEdges;
        this.outEdges = outEdges;
    }

    @Override
    public Iterable<Edge> getEdges(Direction direction, Authorizations authorizations) {
        switch (direction) {
            case BOTH:
                // TODO: Can't we concat the two id lists together and do a single scan, skipping the JoinIterable?
                return new JoinIterable<Edge>(
                        getGraph().getEdges(inEdges.keySet(), authorizations),
                        getGraph().getEdges(outEdges.keySet(), authorizations)
                );
            case IN:
                return getGraph().getEdges(inEdges.keySet(), authorizations);
            case OUT:
                return getGraph().getEdges(outEdges.keySet(), authorizations);
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

    public Iterable<Object> getEdgeIds(final Object otherVertexId, Direction direction, Authorizations authorizations) {
        final Iterable<Map.Entry<Object, EdgeInfo>> edgeInfos = getEdgeInfos(direction, authorizations);
        return new LookAheadIterable<Map.Entry<Object, EdgeInfo>, Object>() {
            @Override
            protected boolean isIncluded(Map.Entry<Object, EdgeInfo> src, Object o) {
                return o != null;
            }

            @Override
            protected Object convert(Map.Entry<Object, EdgeInfo> next) {
                if (next.getValue().getVertexId().equals(otherVertexId)) {
                    return next.getKey();
                }
                return null;
            }

            @Override
            protected Iterator<Map.Entry<Object, EdgeInfo>> createIterator() {
                return edgeInfos.iterator();
            }
        };
    }

    private Iterable<Map.Entry<Object, EdgeInfo>> getEdgeInfos(Direction direction, Authorizations authorizations) {
        switch (direction) {
            case IN:
                return this.inEdges.entrySet();
            case OUT:
                return this.outEdges.entrySet();
            case BOTH:
                return new JoinIterable<Map.Entry<Object, EdgeInfo>>(getEdgeInfos(Direction.IN, authorizations), getEdgeInfos(Direction.OUT, authorizations));
            default:
                throw new SecureGraphException("Unexpected direction: " + direction);
        }
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
                return new JoinIterable<Object>(getVertexIds(Direction.IN, authorizations), getVertexIds(Direction.OUT, authorizations));
            case IN:
                return new ConvertingIterable<EdgeInfo, Object>(this.inEdges.values()) {
                    @Override
                    protected Object convert(EdgeInfo o) {
                        return o.getVertexId();
                    }
                };
            case OUT:
                return new ConvertingIterable<EdgeInfo, Object>(this.outEdges.values()) {
                    @Override
                    protected Object convert(EdgeInfo o) {
                        return o.getVertexId();
                    }
                };
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
        this.outEdges.put(edge.getId(), new EdgeInfo(edge.getLabel(), edge.getVertexId(Direction.IN)));
    }

    void removeOutEdge(Edge edge) {
        this.outEdges.remove(edge.getId());
    }

    void addInEdge(Edge edge) {
        this.inEdges.put(edge.getId(), new EdgeInfo(edge.getLabel(), edge.getVertexId(Direction.OUT)));
    }

    void removeInEdge(Edge edge) {
        this.inEdges.remove(edge.getId());
    }
}
