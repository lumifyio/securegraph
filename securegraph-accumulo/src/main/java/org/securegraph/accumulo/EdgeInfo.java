package org.securegraph.accumulo;

import java.io.Serializable;

public class EdgeInfo implements Serializable {
    private static final long serialVersionUID = 1L;
    private String label;
    private String vertexId;

    protected EdgeInfo() {
    }

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
