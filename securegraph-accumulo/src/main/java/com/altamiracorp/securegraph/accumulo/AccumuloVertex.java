package com.altamiracorp.securegraph.accumulo;

import com.altamiracorp.securegraph.*;
import com.altamiracorp.securegraph.query.VertexQuery;
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
    public static final String ROW_KEY_PREFIX = "V";
    public static final String AFTER_ROW_KEY_PREFIX = "W";
    private final Set<Object> inEdgeIds;
    private final Set<Object> outEdgeIds;
    private final Set<Object> inVertexIds;
    private final Set<Object> outVertexIds;

    AccumuloVertex(AccumuloGraph graph, Object vertexId, Visibility vertexVisibility, Property[] properties) {
        this(graph, vertexId, vertexVisibility, properties, new HashSet<Object>(), new HashSet<Object>(), new HashSet<Object>(), new HashSet<Object>());
    }

    AccumuloVertex(AccumuloGraph graph, Object vertexId, Visibility vertexVisibility, Property[] properties, Set<Object> inEdgeIds, Set<Object> outEdgeIds, Set<Object> inVertexIds, Set<Object> outVertexIds) {
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
                        new EdgesByIdsIterable(getGraph(), inEdgeIds, authorizations),
                        new EdgesByIdsIterable(getGraph(), outEdgeIds, authorizations)
                );
            case IN:
                return new EdgesByIdsIterable(getGraph(), inEdgeIds, authorizations);
            case OUT:
                return new EdgesByIdsIterable(getGraph(), outEdgeIds, authorizations);
            default:
                throw new SecureGraphException("Unexpected direction: " + direction);
        }
    }

    @Override
    public Iterable<Vertex> getVertices(Direction direction, final Authorizations authorizations) {
        switch (direction) {
            case BOTH:
                // TODO: Can't we concat the two id lists together and do a single scan, skipping the JoinIterable?
                return new JoinIterable<Vertex>(
                        new VerticesByIdsIterable(getGraph(), inVertexIds, authorizations),
                        new VerticesByIdsIterable(getGraph(), outVertexIds, authorizations)
                );
            case IN:
                return new VerticesByIdsIterable(getGraph(), inVertexIds, authorizations);
            case OUT:
                return new VerticesByIdsIterable(getGraph(), outVertexIds, authorizations);
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

    void addOutEdge(AccumuloEdge edge) {
        this.outEdgeIds.add(edge.getId().toString());
    }

    void addInEdge(AccumuloEdge edge) {
        this.inEdgeIds.add(edge.getId().toString());
    }

    private static abstract class ElementsByIdsIterable <T> extends LookAheadIterable<Object, T> {
        protected final Graph graph;
        protected final Set<Object> idsList;
        protected final Authorizations authorizations;

        public ElementsByIdsIterable(Graph graph, Set<Object> idsList, Authorizations authorizations) {
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

    private static class EdgesByIdsIterable extends ElementsByIdsIterable<Edge> {
        public EdgesByIdsIterable(Graph graph, Set<Object> idsList, Authorizations authorizations) {
            super(graph, idsList, authorizations);
        }

        @Override
        protected Edge convert(Object id) {
            return graph.getEdge(id, authorizations);
        }
    }

    private static class VerticesByIdsIterable extends ElementsByIdsIterable<Vertex> {
        public VerticesByIdsIterable(Graph graph, Set<Object> idsList, Authorizations authorizations) {
            super(graph, idsList, authorizations);
        }

        @Override
        protected Vertex convert(Object id) {
            return graph.getVertex(id, authorizations);
        }
    }
}
