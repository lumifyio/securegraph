package com.altamiracorp.securegraph;

public abstract class EdgeBase extends ElementBase implements Edge {
    public Object getOutVertexId() {
        return getOutVertex().getId();
    }

    public Object getInVertexId() {
        return getInVertex().getId();
    }

    @Override
    public String toString() {
        return getId() + ":[" + getOutVertexId() + "->" + getInVertexId() + "]";
    }
}
