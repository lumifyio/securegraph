package com.altamiracorp.securegraph;

public abstract class EdgeBuilder extends ElementBuilder<Edge> {
    private Object edgeId;
    private Vertex outVertex;
    private Vertex inVertex;
    private String label;
    private Visibility visibility;

    public EdgeBuilder(Object edgeId, Vertex outVertex, Vertex inVertex, String label, Visibility visibility) {
        this.edgeId = edgeId;
        this.outVertex = outVertex;
        this.inVertex = inVertex;
        this.label = label;
        this.visibility = visibility;
    }

    /**
     * Save the edge along with any properties that were set to the graph.
     *
     * @return The newly created edge.
     */
    @Override
    public abstract Edge save();

    protected Object getEdgeId() {
        return edgeId;
    }

    protected Vertex getOutVertex() {
        return outVertex;
    }

    protected Vertex getInVertex() {
        return inVertex;
    }

    protected String getLabel() {
        return label;
    }

    protected Visibility getVisibility() {
        return visibility;
    }
}
