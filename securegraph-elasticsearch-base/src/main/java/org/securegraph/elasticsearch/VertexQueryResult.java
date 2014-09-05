package org.securegraph.elasticsearch;

import org.securegraph.Property;
import org.securegraph.Visibility;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class VertexQueryResult {
    private String vertexId;
    private Visibility vertexVisibility;
    private List<Property> properties = new ArrayList<Property>();
    private Map<String, EdgeInfo> inEdges = new HashMap<String, EdgeInfo>();
    private Map<String, EdgeInfo> outEdges = new HashMap<String, EdgeInfo>();

    public VertexQueryResult(String vertexId) {
        this.vertexId = vertexId;
    }

    public String getVertexId() {
        return vertexId;
    }

    public Visibility getVertexVisibility() {
        return vertexVisibility;
    }

    public List<Property> getProperties() {
        return properties;
    }

    public Map<String, EdgeInfo> getInEdges() {
        return inEdges;
    }

    public Map<String, EdgeInfo> getOutEdges() {
        return outEdges;
    }

    public void setVertexVisibility(Visibility vertexVisibility) {
        this.vertexVisibility = vertexVisibility;
    }
}
