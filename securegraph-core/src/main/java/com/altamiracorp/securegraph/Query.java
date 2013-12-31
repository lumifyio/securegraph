package com.altamiracorp.securegraph;

public interface Query {
    Iterable<Vertex> vertices();

    Iterable<Edge> edges();
}
