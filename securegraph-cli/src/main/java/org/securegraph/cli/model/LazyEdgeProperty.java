package org.securegraph.cli.model;

import org.securegraph.Edge;
import org.securegraph.Property;
import org.securegraph.Visibility;
import org.securegraph.cli.SecuregraphScript;

public class LazyEdgeProperty extends LazyProperty {
    private final String edgeId;

    public LazyEdgeProperty(SecuregraphScript script, String edgeId, String key, String name, Visibility visibility) {
        super(script, key, name, visibility);
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
