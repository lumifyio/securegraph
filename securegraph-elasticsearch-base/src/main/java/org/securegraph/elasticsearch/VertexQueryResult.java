package org.securegraph.elasticsearch;

import org.securegraph.Property;
import org.securegraph.Visibility;

import java.util.Map;

public class VertexQueryResult {
    private String vertexId;
    private Visibility vertexVisibility;
    private Iterable<Property> properties;
    private Map<String, EdgeInfo> inEdges;
    private Map<String, EdgeInfo> outEdges;

    public String getVertexId() {
        return vertexId;
    }

    public Visibility getVertexVisibility() {
        return vertexVisibility;
    }

    public Iterable<Property> getProperties() {
        return properties;
    }

    public Map<String, EdgeInfo> getInEdges() {
        return inEdges;
    }

    public Map<String, EdgeInfo> getOutEdges() {
        return outEdges;
    }
}
