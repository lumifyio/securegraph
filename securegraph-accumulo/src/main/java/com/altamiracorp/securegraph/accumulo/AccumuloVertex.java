package com.altamiracorp.securegraph.accumulo;

import com.altamiracorp.securegraph.*;
import com.altamiracorp.securegraph.query.DefaultVertexQuery;
import com.altamiracorp.securegraph.query.VertexQuery;
import org.apache.hadoop.io.Text;

public class AccumuloVertex extends AccumuloElement implements Vertex {
    public static final Text CF_SIGNAL = new Text("V");

    public AccumuloVertex(AccumuloGraph graph, Object vertexId, Visibility vertexVisibility) {
        super(graph, vertexId, vertexVisibility);
    }

    public Text getRowKey() {
        throw new RuntimeException("not implemented");
    }

    @Override
    public Iterable<Property> getProperties() {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void setProperties(Property... properties) {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void addProperties(Property... properties) {
        throw new RuntimeException("not implemented");
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
