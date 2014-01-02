package com.altamiracorp.securegraph;

public interface Edge extends Element {
    String getLabel();

    Object getOutVertexId();

    Vertex getOutVertex(Authorizations authorizations);

    Object getInVertexId();

    Vertex getInVertex(Authorizations authorizations);
}
