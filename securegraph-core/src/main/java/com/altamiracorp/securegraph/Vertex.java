package com.altamiracorp.securegraph;

import com.altamiracorp.securegraph.query.VertexQuery;

public interface Vertex extends Element {
    Iterable<Edge> getEdges(Direction direction);

    VertexQuery query();
}
