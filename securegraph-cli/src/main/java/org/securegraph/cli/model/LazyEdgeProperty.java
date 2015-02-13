package org.securegraph.cli.model;

import org.securegraph.Edge;
import org.securegraph.Property;
import org.securegraph.Visibility;

public class LazyEdgeProperty extends LazyProperty {
    private final String edgeId;

    public LazyEdgeProperty(String edgeId, String key, String name, Visibility visibility) {
        super(key, name, visibility);
        this.edgeId = edgeId;
    }

    @Override
    protected String getToStringHeaderLine() {
        return "edge @|bold " + getEdgeId() + "|@ property";
    }

    @Override
    protected Property getP() {
        Edge edge = getGraph().getEdge(getEdgeId(), getAuthorizations());
        if (edge == null) {
            return null;
        }
        return edge.getProperty(getKey(), getName(), getVisibility());
    }

    public String getEdgeId() {
        return edgeId;
    }
}
