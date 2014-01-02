package com.altamiracorp.securegraph.accumulo;

import com.altamiracorp.securegraph.*;
import com.altamiracorp.securegraph.query.DefaultVertexQuery;
import com.altamiracorp.securegraph.query.VertexQuery;
import org.apache.hadoop.io.Text;

import java.util.ArrayList;
import java.util.List;

public class AccumuloVertex extends AccumuloElement implements Vertex {
    public static final Text CF_SIGNAL = new Text("V");
    public static final Text CF_OUT_EDGE = new Text("VOUT");
    public static final Text CF_IN_EDGE = new Text("VIN");
    public static final String ROW_KEY_PREFIX = "V";
    public static final String AFTER_ROW_KEY_PREFIX = "W";
    private final List<String> inEdgeIds;
    private final List<String> outEdgeIds;

    AccumuloVertex(AccumuloGraph graph, Object vertexId, Visibility vertexVisibility, Property[] properties) {
        this(graph, vertexId, vertexVisibility, properties, new ArrayList<String>(), new ArrayList<String>());
    }

    AccumuloVertex(AccumuloGraph graph, Object vertexId, Visibility vertexVisibility, Property[] properties, List<String> inEdgeIds, List<String> outEdgeIds) {
        super(graph, vertexId, vertexVisibility, properties);
        this.inEdgeIds = inEdgeIds;
        this.outEdgeIds = outEdgeIds;
    }

    @Override
    public Iterable<Edge> getEdges(Direction direction) {
        throw new RuntimeException("not implemented");
    }

    @Override
    public VertexQuery query(Authorizations authorizations) {
        return new DefaultVertexQuery(getGraph(), authorizations, this);
    }
}
