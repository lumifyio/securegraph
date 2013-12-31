package com.altamiracorp.securegraph;

public abstract class EdgeBase extends ElementBase implements Edge {
    public abstract String getLabel();

    public Object getOutVertexId() {
        return getOutVertex().getId();
    }

    public abstract Vertex getOutVertex();

    public Object getInVertexId() {
        return getInVertex().getId();
    }

    public abstract Vertex getInVertex();

    @Override
    public String toString() {
        return getId() + ":[" + getOutVertexId() + "->" + getInVertexId() + "]";
    }
}
