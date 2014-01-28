package com.altamiracorp.securegraph.accumulo;

import org.apache.accumulo.core.data.Value;

import java.io.Serializable;

public class EdgeInfo implements Serializable {
    private String label;
    private Object vertexId;

    public EdgeInfo(String label, Object vertexId) {
        this.label = label;
        this.vertexId = vertexId;
    }

    public String getLabel() {
        return label;
    }

    public Object getVertexId() {
        return vertexId;
    }
}
