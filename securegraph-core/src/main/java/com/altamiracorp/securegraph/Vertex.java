package com.altamiracorp.securegraph;

import com.altamiracorp.securegraph.query.VertexQuery;

public interface Vertex extends Element {
    Iterable<Edge> getEdges(Direction direction, Authorizations authorizations);

    Iterable<Edge> getEdges(Direction direction, String label, Authorizations authorizations);

    Iterable<Edge> getEdges(Direction direction, String[] labels, Authorizations authorizations);

    Iterable<Edge> getEdges(Vertex otherVertex, Direction direction, Authorizations authorizations);

    Iterable<Edge> getEdges(Vertex otherVertex, Direction direction, String label, Authorizations authorizations);

    Iterable<Edge> getEdges(Vertex otherVertex, Direction direction, String[] labels, Authorizations authorizations);

    Iterable<Vertex> getVertices(Direction direction, Authorizations authorizations);

    Iterable<Vertex> getVertices(Direction direction, String label, Authorizations authorizations);

    Iterable<Vertex> getVertices(Direction direction, String[] labels, Authorizations authorizations);

    VertexQuery query(Authorizations authorizations);

    VertexQuery query(String queryString, Authorizations authorizations);

    ElementMutation<Vertex> prepareMutation();
}
