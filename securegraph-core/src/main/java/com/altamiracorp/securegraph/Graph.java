package com.altamiracorp.securegraph;

import com.altamiracorp.securegraph.query.GraphQuery;

public interface Graph {
    Vertex addVertex(Visibility visibility, Property... properties);

    Vertex addVertex(Object vertexId, Visibility visibility, Property... properties);

    Vertex getVertex(Object vertexId, Authorizations authorizations);

    Iterable<Vertex> getVertices(Authorizations authorizations);

    void removeVertex(Vertex vertex, Authorizations authorizations);

    void removeVertex(String vertexId, Authorizations authorizations);

    Edge addEdge(Vertex outVertex, Vertex inVertex, String label, Visibility visibility, Property... properties);

    Edge addEdge(Object edgeId, Vertex outVertex, Vertex inVertex, String label, Visibility visibility, Property... properties);

    Edge getEdge(Object edgeId, Authorizations authorizations);

    Iterable<Edge> getEdges(Authorizations authorizations);

    void removeEdge(Edge edge, Authorizations authorizations);

    void removeEdge(String edgeId, Authorizations authorizations);

    GraphQuery query(Authorizations authorizations);
}
