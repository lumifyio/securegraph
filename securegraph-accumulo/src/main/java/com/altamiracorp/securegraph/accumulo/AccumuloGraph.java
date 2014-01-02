package com.altamiracorp.securegraph.accumulo;

import com.altamiracorp.securegraph.*;

public class AccumuloGraph extends GraphBase {

    @Override
    public Vertex addVertex(Object vertexId, Visibility visibility, Property... properties) {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public Iterable<Vertex> getVertices(Authorizations authorizations) {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void removeVertex(Object vertexId, Authorizations authorizations) {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public Edge addEdge(Object edgeId, Vertex outVertex, Vertex inVertex, String label, Visibility visibility, Property... properties) {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public Iterable<Edge> getEdges(Authorizations authorizations) {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void removeEdge(Object edgeId, Authorizations authorizations) {
        throw new RuntimeException("Not implemented");
    }
}
