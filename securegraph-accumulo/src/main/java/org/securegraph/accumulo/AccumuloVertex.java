package org.securegraph.accumulo;

import org.apache.hadoop.io.Text;
import org.securegraph.*;
import org.securegraph.mutation.ExistingElementMutation;
import org.securegraph.mutation.ExistingElementMutationImpl;
import org.securegraph.query.VertexQuery;
import org.securegraph.util.ConvertingIterable;
import org.securegraph.util.JoinIterable;
import org.securegraph.util.LookAheadIterable;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.securegraph.util.IterableUtils.count;
import static org.securegraph.util.IterableUtils.toSet;

public class AccumuloVertex extends AccumuloElement<Vertex> implements Vertex {
    public static final Text CF_SIGNAL = new Text("V");
    public static final Text CF_OUT_EDGE = new Text("EOUT");
    public static final Text CF_OUT_EDGE_HIDDEN = new Text("EOUTH");
    public static final Text CF_IN_EDGE = new Text("EIN");
    public static final Text CF_IN_EDGE_HIDDEN = new Text("EINH");
    private final Map<String, EdgeInfo> inEdges;
    private final Map<String, EdgeInfo> outEdges;

    public AccumuloVertex(AccumuloGraph graph, String vertexId, Visibility vertexVisibility, Iterable<Property> properties, Authorizations authorizations) {
        this(graph, vertexId, vertexVisibility, properties, new HashMap<String, EdgeInfo>(), new HashMap<String, EdgeInfo>(), authorizations);
    }

    AccumuloVertex(AccumuloGraph graph, String vertexId, Visibility vertexVisibility, Iterable<Property> properties, Map<String, EdgeInfo> inEdges, Map<String, EdgeInfo> outEdges, Authorizations authorizations) {
        super(graph, vertexId, vertexVisibility, properties, authorizations);
        this.inEdges = inEdges;
        this.outEdges = outEdges;
    }

    @Override
    public Iterable<Edge> getEdges(Direction direction, Authorizations authorizations) {
        return getEdges(direction, FetchHint.ALL, authorizations);
    }

    @Override
    public Iterable<Edge> getEdges(Direction direction, EnumSet<FetchHint> fetchHints, Authorizations authorizations) {
        return getGraph().getEdges(getEdgeIds(direction, authorizations), fetchHints, authorizations);
    }

    @Override
    public Iterable<String> getEdgeIds(Direction direction, Authorizations authorizations) {
        return getEdgeIdsWithOtherVertexId(null, direction, null, authorizations);
    }

    @Override
    public Iterable<Edge> getEdges(Direction direction, String label, Authorizations authorizations) {
        return getEdges(direction, label, FetchHint.ALL, authorizations);
    }

    @Override
    public Iterable<Edge> getEdges(Direction direction, String label, EnumSet<FetchHint> fetchHints, Authorizations authorizations) {
        return getGraph().getEdges(getEdgeIds(direction, new String[]{label}, authorizations), fetchHints, authorizations);
    }

    @Override
    public Iterable<String> getEdgeIds(Direction direction, String label, Authorizations authorizations) {
        return getEdgeIdsWithOtherVertexId(null, direction, new String[]{label}, authorizations);
    }

    @Override
    public Iterable<Edge> getEdges(Direction direction, String[] labels, Authorizations authorizations) {
        return getEdges(direction, labels, FetchHint.ALL, authorizations);
    }

    @Override
    public Iterable<Edge> getEdges(Direction direction, final String[] labels, EnumSet<FetchHint> fetchHints, Authorizations authorizations) {
        return getGraph().getEdges(getEdgeIdsWithOtherVertexId(null, direction, labels, authorizations), fetchHints, authorizations);
    }

    @Override
    public Iterable<String> getEdgeIds(final Direction direction, final String[] labels, final Authorizations authorizations) {
        return getEdgeIdsWithOtherVertexId(null, direction, labels, authorizations);
    }

    @Override
    public Iterable<Edge> getEdges(Vertex otherVertex, Direction direction, Authorizations authorizations) {
        return getEdges(otherVertex, direction, FetchHint.ALL, authorizations);
    }

    @Override
    public Iterable<Edge> getEdges(final Vertex otherVertex, Direction direction, EnumSet<FetchHint> fetchHints, Authorizations authorizations) {
        return getGraph().getEdges(getEdgeIdsWithOtherVertexId(otherVertex.getId(), direction, null, authorizations), fetchHints, authorizations);
    }

    @Override
    public Iterable<String> getEdgeIds(Vertex otherVertex, Direction direction, Authorizations authorizations) {
        return getEdgeIdsWithOtherVertexId(otherVertex.getId(), direction, null, authorizations);
    }

    @Override
    public Iterable<Edge> getEdges(Vertex otherVertex, Direction direction, String label, Authorizations authorizations) {
        return getEdges(otherVertex, direction, label, FetchHint.ALL, authorizations);
    }

    @Override
    public Iterable<Edge> getEdges(final Vertex otherVertex, Direction direction, String label, EnumSet<FetchHint> fetchHints, Authorizations authorizations) {
        return getGraph().getEdges(getEdgeIdsWithOtherVertexId(otherVertex.getId(), direction, new String[]{label}, authorizations), fetchHints, authorizations);
    }

    @Override
    public Iterable<String> getEdgeIds(Vertex otherVertex, Direction direction, String label, Authorizations authorizations) {
        return getEdgeIdsWithOtherVertexId(otherVertex.getId(), direction, new String[]{label}, authorizations);
    }

    @Override
    public Iterable<Edge> getEdges(Vertex otherVertex, Direction direction, String[] labels, Authorizations authorizations) {
        return getEdges(otherVertex, direction, labels, FetchHint.ALL, authorizations);
    }

    @Override
    public Iterable<Edge> getEdges(final Vertex otherVertex, Direction direction, String[] labels, EnumSet<FetchHint> fetchHints, Authorizations authorizations) {
        return getGraph().getEdges(getEdgeIdsWithOtherVertexId(otherVertex.getId(), direction, labels, authorizations), fetchHints, authorizations);
    }

    @Override
    public Iterable<String> getEdgeIds(final Vertex otherVertex, final Direction direction, final String[] labels, final Authorizations authorizations) {
        return getEdgeIdsWithOtherVertexId(otherVertex.getId(), direction, labels, authorizations);
    }

    @Override
    public int getEdgeCount(Direction direction, Authorizations authorizations) {
        return count(getEdgeIds(direction, authorizations));
    }

    @Override
    public Iterable<String> getEdgeLabels(Direction direction, Authorizations authorizations) {
        return toSet(new ConvertingIterable<Map.Entry<String, EdgeInfo>, String>(getEdgeInfos(direction, authorizations)) {
            @Override
            protected String convert(Map.Entry<String, EdgeInfo> o) {
                return o.getValue().getLabel();
            }
        });
    }

    @Override
    public Iterable<Vertex> getVertices(Direction direction, Authorizations authorizations) {
        return getVertices(direction, FetchHint.ALL, authorizations);
    }

    public Iterable<String> getEdgeIdsWithOtherVertexId(final String otherVertexId, final Direction direction, final String[] labels, final Authorizations authorizations) {
        return new LookAheadIterable<Map.Entry<String, EdgeInfo>, String>() {
            @Override
            protected boolean isIncluded(Map.Entry<String, EdgeInfo> edgeInfo, String edgeId) {
                if (otherVertexId != null) {
                    if (!otherVertexId.equals(edgeInfo.getValue().getVertexId())) {
                        return false;
                    }
                }
                if (labels == null || labels.length == 0) {
                    return true;
                }

                for (String label : labels) {
                    if (label.equals(edgeInfo.getValue().getLabel())) {
                        return true;
                    }
                }
                return false;
            }

            @Override
            protected String convert(Map.Entry<String, EdgeInfo> edgeInfo) {
                return edgeInfo.getKey();
            }

            @Override
            protected Iterator<Map.Entry<String, EdgeInfo>> createIterator() {
                return getEdgeInfos(direction, authorizations).iterator();
            }
        };
    }

    private Iterable<Map.Entry<String, EdgeInfo>> getEdgeInfos(Direction direction, Authorizations authorizations) {
        switch (direction) {
            case IN:
                return this.inEdges.entrySet();
            case OUT:
                return this.outEdges.entrySet();
            case BOTH:
                return new JoinIterable<Map.Entry<String, EdgeInfo>>(this.inEdges.entrySet(), this.outEdges.entrySet());
            default:
                throw new SecureGraphException("Unexpected direction: " + direction);
        }
    }

    @Override
    public Iterable<Vertex> getVertices(Direction direction, EnumSet<FetchHint> fetchHints, final Authorizations authorizations) {
        return getGraph().getVertices(getVertexIds(direction, authorizations), fetchHints, authorizations);
    }

    @Override
    public Iterable<Vertex> getVertices(Direction direction, String label, Authorizations authorizations) {
        return getVertices(direction, label, FetchHint.ALL, authorizations);
    }

    @Override
    public Iterable<Vertex> getVertices(Direction direction, String label, EnumSet<FetchHint> fetchHints, Authorizations authorizations) {
        return getVertices(direction, new String[]{label}, fetchHints, authorizations);
    }

    @Override
    public Iterable<Vertex> getVertices(Direction direction, String[] labels, Authorizations authorizations) {
        return getVertices(direction, labels, FetchHint.ALL, authorizations);
    }

    @Override
    public Iterable<Vertex> getVertices(Direction direction, String[] labels, EnumSet<FetchHint> fetchHints, final Authorizations authorizations) {
        return getGraph().getVertices(getVertexIds(direction, labels, authorizations), fetchHints, authorizations);
    }

    @Override
    public Iterable<String> getVertexIds(Direction direction, String label, Authorizations authorizations) {
        return getVertexIds(direction, new String[]{label}, authorizations);
    }

    @Override
    public Iterable<String> getVertexIds(Direction direction, Authorizations authorizations) {
        return getVertexIds(direction, (String[]) null, authorizations);
    }

    @Override
    public Iterable<String> getVertexIds(Direction direction, String[] labels, Authorizations authorizations) {
        switch (direction) {
            case BOTH:
                return new JoinIterable<String>(getVertexIds(Direction.IN, labels, authorizations), getVertexIds(Direction.OUT, labels, authorizations));
            case IN:
                return new GetVertexIdsIterable(this.inEdges.values(), labels);
            case OUT:
                return new GetVertexIdsIterable(this.outEdges.values(), labels);
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

    @Override
    public ExistingElementMutation<Vertex> prepareMutation() {
        return new ExistingElementMutationImpl<Vertex>(this) {
            @Override
            public Vertex save(Authorizations authorizations) {
                saveExistingElementMutation(this, authorizations);
                return getElement();
            }
        };
    }
}
