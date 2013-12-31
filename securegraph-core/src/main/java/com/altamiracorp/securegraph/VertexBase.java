package com.altamiracorp.securegraph;

public abstract class VertexBase extends ElementBase implements Vertex {
    public abstract Iterable<Edge> getEdges(Direction direction);
}
