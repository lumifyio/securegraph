package com.altamiracorp.securegraph;

public interface Edge extends Element {
    String getLabel();

    Object getVertexId(Direction direction);

    Vertex getVertex(Direction direction, Authorizations authorizations);
}
