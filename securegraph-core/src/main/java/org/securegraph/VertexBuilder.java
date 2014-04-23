package org.securegraph;

public abstract class VertexBuilder extends ElementBuilder<Vertex> {
    private Object vertexId;
    private Visibility visibility;

    public VertexBuilder(Object vertexId, Visibility visibility) {
        this.vertexId = vertexId;
        this.visibility = visibility;
    }

    /**
     * Save the vertex along with any properties that were set to the graph.
     *
     * @return The newly created vertex.
     */
    @Override
    public abstract Vertex save();

    protected Object getVertexId() {
        return vertexId;
    }

    protected Visibility getVisibility() {
        return visibility;
    }
}
