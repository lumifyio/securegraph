package com.altamiracorp.securegraph;

public interface Edge extends Element {
    String getLabel();

    Object getVertexId(Direction direction);

    Vertex getVertex(Direction direction, Authorizations authorizations);

    /**
     * Given a vertexId that represents one side of a relationship, get me the id of the other side.
     */
    Object getOtherVertexId(Object myVertexId);

    /**
     * Given a vertexId that represents one side of a relationship, get me the vertex of the other side.
     */
    Vertex getOtherVertex(Object myVertexId, Authorizations authorizations);
}
