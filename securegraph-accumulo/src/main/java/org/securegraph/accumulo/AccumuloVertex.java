package org.securegraph.accumulo;

import org.apache.hadoop.io.Text;
import org.securegraph.*;
import org.securegraph.mutation.ExistingElementMutation;
import org.securegraph.mutation.ExistingElementMutationImpl;
import org.securegraph.query.VertexQuery;
import org.securegraph.util.ConvertingIterable;
import org.securegraph.util.JoinIterable;
import org.securegraph.util.LookAheadIterable;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.securegraph.util.IterableUtils.count;
import static org.securegraph.util.IterableUtils.toSet;

public class AccumuloVertex extends AccumuloElement<Vertex> implements Vertex {
    public static final Text CF_SIGNAL = new Text("V");
    public static final Text CF_OUT_EDGE = new Text("EOUT");
    public static final Text CF_IN_EDGE = new Text("EIN");
    private final Map<Object, EdgeInfo> inEdges;
    private final Map<Object, EdgeInfo> outEdges;

    public AccumuloVertex(AccumuloGraph graph, Object vertexId, Visibility vertexVisibility, Iterable<Property> properties, Authorizations authorizations) {
        this(graph, vertexId, vertexVisibility, properties, new HashMap<Object, EdgeInfo>(), new HashMap<Object, EdgeInfo>(), authorizations);
    }

    AccumuloVertex(AccumuloGraph graph, Object vertexId, Visibility vertexVisibility, Iterable<Property> properties, Map<Object, EdgeInfo> inEdges, Map<Object, EdgeInfo> outEdges, Authorizations authorizations) {
        super(graph, vertexId, vertexVisibility, properties, authorizations);
        this.inEdges = inEdges;
        this.outEdges = outEdges;
    }

    @Override
    public Iterable<Edge> getEdges(Direction direction, Authorizations authorizations) {
        return getGraph().getEdges(getEdgeIds(direction, authorizations), authorizations);
    }

    @Override
    public Iterable<Object> getEdgeIds(Direction direction, Authorizations authorizations) {
        return getEdgeIdsWithOtherVertexId(null, direction, null, authorizations);
    }

    @Override
    public Iterable<Edge> getEdges(Direction direction, String label, Authorizations authorizations) {
        return getGraph().getEdges(getEdgeIds(direction, new String[]{label}, authorizations), authorizations);
    }

    @Override
    public Iterable<Object> getEdgeIds(Direction direction, String label, Authorizations authorizations) {
        return getEdgeIdsWithOtherVertexId(null, direction, new String[]{label}, authorizations);
    }

    @Override
    public Iterable<Edge> getEdges(Direction direction, final String[] labels, Authorizations authorizations) {
        return getGraph().getEdges(getEdgeIdsWithOtherVertexId(null, direction, labels, authorizations), authorizations);
    }

    @Override
    public Iterable<Object> getEdgeIds(final Direction direction, final String[] labels, final Authorizations authorizations) {
        return getEdgeIdsWithOtherVertexId(null, direction, labels, authorizations);
    }

    @Override
    public Iterable<Edge> getEdges(final Vertex otherVertex, Direction direction, Authorizations authorizations) {
        return getGraph().getEdges(getEdgeIdsWithOtherVertexId(otherVertex.getId(), direction, null, authorizations), authorizations);
    }

    @Override
    public Iterable<Object> getEdgeIds(Vertex otherVertex, Direction direction, Authorizations authorizations) {
        return getEdgeIdsWithOtherVertexId(otherVertex.getId(), direction, null, authorizations);
    }

    @Override
    public Iterable<Edge> getEdges(final Vertex otherVertex, Direction direction, String label, Authorizations authorizations) {
        return getGraph().getEdges(getEdgeIdsWithOtherVertexId(otherVertex.getId(), direction, new String[]{label}, authorizations), authorizations);
    }

    @Override
    public Iterable<Object> getEdgeIds(Vertex otherVertex, Direction direction, String label, Authorizations authorizations) {
        return getEdgeIdsWithOtherVertexId(otherVertex.getId(), direction, new String[]{label}, authorizations);
    }

    @Override
    public Iterable<Edge> getEdges(final Vertex otherVertex, Direction direction, String[] labels, Authorizations authorizations) {
        return getGraph().getEdges(getEdgeIdsWithOtherVertexId(otherVertex.getId(), direction, labels, authorizations), authorizations);
    }

    @Override
    public Iterable<Object> getEdgeIds(final Vertex otherVertex, final Direction direction, final String[] labels, final Authorizations authorizations) {
        return getEdgeIdsWithOtherVertexId(otherVertex.getId(), direction, labels, authorizations);
    }

    @Override
    public int getEdgeCount(Direction direction, Authorizations authorizations) {
        return count(getEdgeIds(direction, authorizations));
    }

    @Override
    public Iterable<String> getEdgeLabels(Direction direction, Authorizations authorizations) {
        return toSet(new ConvertingIterable<Map.Entry<Object, EdgeInfo>, String>(getEdgeInfos(direction, authorizations)) {
            @Override
            protected String convert(Map.Entry<Object, EdgeInfo> o) {
                return o.getValue().getLabel();
            }
        });
    }

    public Iterable<Object> getEdgeIdsWithOtherVertexId(final Object otherVertexId, final Direction direction, final String[] labels, final Authorizations authorizations) {
        return new LookAheadIterable<Map.Entry<Object, EdgeInfo>, Object>() {
            @Override
            protected boolean isIncluded(Map.Entry<Object, EdgeInfo> edgeInfo, Object edgeId) {
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
            protected Object convert(Map.Entry<Object, EdgeInfo> edgeInfo) {
                return edgeInfo.getKey();
            }

            @Override
            protected Iterator<Map.Entry<Object, EdgeInfo>> createIterator() {
                return getEdgeInfos(direction, authorizations).iterator();
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
                return new JoinIterable<Map.Entry<Object, EdgeInfo>>(this.inEdges.entrySet(), this.outEdges.entrySet());
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
        return getGraph().getVertices(getVertexIds(direction, labels, authorizations), authorizations);
    }

    @Override
    public Iterable<Object> getVertexIds(Direction direction, String label, Authorizations authorizations) {
        return getVertexIds(direction, new String[]{label}, authorizations);
    }

    @Override
    public Iterable<Object> getVertexIds(Direction direction, Authorizations authorizations) {
        return getVertexIds(direction, (String[]) null, authorizations);
    }

    @Override
    public Iterable<Object> getVertexIds(Direction direction, String[] labels, Authorizations authorizations) {
        switch (direction) {
            case BOTH:
                return new JoinIterable<Object>(getVertexIds(Direction.IN, labels, authorizations), getVertexIds(Direction.OUT, labels, authorizations));
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
