package com.altamiracorp.securegraph;

public interface VertexQuery extends Query {
    Iterable<Edge> edges(Direction direction);
}
