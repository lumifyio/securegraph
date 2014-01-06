package com.altamiracorp.securegraph;

import com.altamiracorp.securegraph.query.VertexQuery;

public interface Vertex extends Element {
    Iterable<Edge> getEdges(Direction direction, Authorizations authorizations);

    Iterable<Vertex> getVertices(Direction direction, Authorizations authorizations);

    VertexQuery query(Authorizations authorizations);

    VertexQuery query(String queryString, Authorizations authorizations);
}
