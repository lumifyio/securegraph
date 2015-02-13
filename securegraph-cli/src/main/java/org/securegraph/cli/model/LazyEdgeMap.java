package org.securegraph.cli.model;

import org.securegraph.Edge;

public class LazyEdgeMap extends ModelBase {
    public LazyEdge get(String edgeId) {
        Edge e = getGraph().getEdge(edgeId, getAuthorizations());
        if (e == null) {
            return null;
        }
        return new LazyEdge(edgeId);
    }
}
