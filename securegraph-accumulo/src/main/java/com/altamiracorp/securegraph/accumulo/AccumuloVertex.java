package com.altamiracorp.securegraph.accumulo;

import com.altamiracorp.securegraph.*;
import com.altamiracorp.securegraph.query.DefaultVertexQuery;
import com.altamiracorp.securegraph.query.VertexQuery;
import com.altamiracorp.securegraph.util.LookAheadIterable;
import org.apache.hadoop.io.Text;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class AccumuloVertex extends AccumuloElement implements Vertex {
    public static final Text CF_SIGNAL = new Text("V");
    public static final Text CF_OUT_EDGE = new Text("VOUT");
    public static final Text CF_IN_EDGE = new Text("VIN");
    public static final String ROW_KEY_PREFIX = "V";
    public static final String AFTER_ROW_KEY_PREFIX = "W";
    private final List<Object> inEdgeIds;
    private final List<Object> outEdgeIds;

    AccumuloVertex(AccumuloGraph graph, Object vertexId, Visibility vertexVisibility, Property[] properties) {
        this(graph, vertexId, vertexVisibility, properties, new ArrayList<Object>(), new ArrayList<Object>());
    }

    AccumuloVertex(AccumuloGraph graph, Object vertexId, Visibility vertexVisibility, Property[] properties, List<Object> inEdgeIds, List<Object> outEdgeIds) {
        super(graph, vertexId, vertexVisibility, properties);
        this.inEdgeIds = inEdgeIds;
        this.outEdgeIds = outEdgeIds;
    }

    @Override
    public Iterable<Edge> getEdges(Direction direction, Authorizations authorizations) {
        switch (direction) {
            case BOTH:
                // TODO it would be nice if we didn't have to create a new list here
                List<Object> ids = new ArrayList<Object>();
                ids.addAll(inEdgeIds);
                ids.addAll(outEdgeIds);
                return new EdgesByIdsIterable(getGraph(), ids, authorizations);
            case IN:
                return new EdgesByIdsIterable(getGraph(), inEdgeIds, authorizations);
            case OUT:
                return new EdgesByIdsIterable(getGraph(), outEdgeIds, authorizations);
            default:
                throw new SecureGraphException("Unexpected direction: " + direction);
        }
    }

    @Override
    public VertexQuery query(Authorizations authorizations) {
        return new DefaultVertexQuery(getGraph(), authorizations, this);
    }

    void addOutEdge(AccumuloEdge edge) {
        this.outEdgeIds.add(edge.getId().toString());
    }

    void addInEdge(AccumuloEdge edge) {
        this.inEdgeIds.add(edge.getId().toString());
    }

    private static class EdgesByIdsIterable extends LookAheadIterable<Object, Edge> {
        private final Graph graph;
        private final List<Object> idsList;
        private final Authorizations authorizations;

        public EdgesByIdsIterable(Graph graph, List<Object> idsList, Authorizations authorizations) {
            this.graph = graph;
            this.idsList = idsList;
            this.authorizations = authorizations;
        }

        @Override
        protected boolean isIncluded(Edge obj) {
            return obj != null;
        }

        @Override
        protected Edge convert(Object edgeId) {
            return graph.getEdge(edgeId, authorizations);
        }

        @Override
        protected Iterator<Object> createIterator() {
            return idsList.iterator();
        }
    }
}
