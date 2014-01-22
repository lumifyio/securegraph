package com.altamiracorp.securegraph;

public abstract class VertexBuilder extends ElementBuilder<Vertex> {
    private Object vertexId;
    private Visibility visibility;

    public VertexBuilder(Object vertexId, Visibility visibility) {
        this.vertexId = vertexId;
        this.visibility = visibility;
    }

    @Override
    public abstract Vertex save();

    protected Object getVertexId() {
        return vertexId;
    }

    protected Visibility getVisibility() {
        return visibility;
    }
}
