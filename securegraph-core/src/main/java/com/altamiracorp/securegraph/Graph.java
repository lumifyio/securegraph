package com.altamiracorp.securegraph;

public interface Graph {
    Vertex addVertex(Visibility visibility);

    Vertex addVertex(Object vertexId, Visibility visibility, Property... properties);

    Vertex getVertex(Object vertexId, Authorizations authorizations);

    Iterable<Vertex> getVertices(Authorizations authorizations);

    void removeVertex(Object vertexId, Authorizations authorizations);

    Edge addEdge(Vertex outVertex, Vertex inVertex, String label, Visibility visibility);

    Edge addEdge(Object edgeId, Vertex outVertex, Vertex inVertex, String label, Visibility visibility, Property... properties);

    Iterable<Edge> getEdges(Authorizations authorizations);

    void removeEdge(Object edgeId, Authorizations authorizations);

    Edge getEdge(Object edgeId, Authorizations authorizations);
}
