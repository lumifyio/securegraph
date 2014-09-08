package org.securegraph.elasticsearch;

import org.securegraph.*;
import org.securegraph.mutation.ExistingElementMutation;
import org.securegraph.mutation.ExistingElementMutationImpl;
import org.securegraph.query.VertexQuery;
import org.securegraph.util.JoinIterable;
import org.securegraph.util.LookAheadIterable;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.securegraph.util.IterableUtils.count;

public class ElasticSearchVertex extends ElasticSearchElement<Vertex> implements Vertex {
    private final Map<String, EdgeInfo> inEdges;
    private final Map<String, EdgeInfo> outEdges;

    protected ElasticSearchVertex(Graph graph, String id, Visibility visibility, Iterable<Property> properties, Authorizations authorizations) {
        this(graph, id, visibility, properties, new HashMap<String, EdgeInfo>(), new HashMap<String, EdgeInfo>(), authorizations);
    }

    ElasticSearchVertex(Graph graph, String vertexId, Visibility vertexVisibility, Iterable<Property> properties, Map<String, EdgeInfo> inEdges, Map<String, EdgeInfo> outEdges, Authorizations authorizations) {
        super(graph, vertexId, vertexVisibility, properties, authorizations);
        this.inEdges = inEdges;
        this.outEdges = outEdges;
    }

    public ElasticSearchVertex(Graph graph, VertexQueryResult vertexQueryResult, Authorizations authorizations) {
        this(
                graph,
                vertexQueryResult.getVertexId(),
                vertexQueryResult.getVertexVisibility(),
                vertexQueryResult.getProperties(),
                vertexQueryResult.getInEdges(),
                vertexQueryResult.getOutEdges(),
                authorizations);
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
        throw new RuntimeException("not implemented");
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
        throw new RuntimeException("not implemented");
    }

    @Override
    public VertexQuery query(Authorizations authorizations) {
        return query(null, authorizations);
    }

    @Override
    public VertexQuery query(String queryString, Authorizations authorizations) {
        throw new RuntimeException("not implemented");
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
