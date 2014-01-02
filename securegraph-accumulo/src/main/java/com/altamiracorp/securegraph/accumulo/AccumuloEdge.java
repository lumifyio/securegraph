package com.altamiracorp.securegraph.accumulo;

import com.altamiracorp.securegraph.*;
import org.apache.hadoop.io.Text;

public class AccumuloEdge extends AccumuloElement implements Edge {
    public static final Text CF_SIGNAL = new Text("E");

    protected AccumuloEdge(Graph graph, Object id, Visibility visibility, Property[] properties) {
        super(graph, id, visibility, properties);
    }

    @Override
    public String getLabel() {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public Object getOutVertexId() {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public Vertex getOutVertex() {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public Object getInVertexId() {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public Vertex getInVertex() {
        throw new RuntimeException("Not implemented");
    }
}
