package org.securegraph;

public abstract class EdgeBuilderBase extends ElementBuilder<Edge> {
    private final String edgeId;
    private final String label;
    private final Visibility visibility;

    protected EdgeBuilderBase(String edgeId, String label, Visibility visibility) {
        this.edgeId = edgeId;
        this.label = label;
        this.visibility = visibility;
    }

    public String getEdgeId() {
        return edgeId;
    }

    public String getLabel() {
        return label;
    }

    public Visibility getVisibility() {
        return visibility;
    }

    /**
     * Save the edge along with any properties that were set to the graph.
     *
     * @return The newly created edge.
     */
    @Override
    public abstract Edge save(Authorizations authorizations);
}
