package org.securegraph.elasticsearch;

public class EdgeInfo {
    private final String label;
    private final String vertexId;

    public EdgeInfo(String label, String vertexId) {
        this.label = label;
        this.vertexId = vertexId;
    }

    public String getLabel() {
        return label;
    }

    public String getVertexId() {
        return vertexId;
    }
}
