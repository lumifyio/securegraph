package com.altamiracorp.securegraph;

public interface Edge extends Element {
    String getLabel();

    Object getOutVertexId();

    Vertex getOutVertex();

    Object getInVertexId();

    Vertex getInVertex();
}
