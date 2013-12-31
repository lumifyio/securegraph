package com.altamiracorp.securegraph;

public interface Vertex extends Element {
    Iterable<Edge> getEdges(Direction direction);
}
