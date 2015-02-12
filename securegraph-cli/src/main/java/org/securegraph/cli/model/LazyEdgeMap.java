package org.securegraph.cli.model;

import org.securegraph.Edge;
import org.securegraph.cli.SecuregraphScript;

public class LazyEdgeMap extends ModelBase {
    public LazyEdgeMap(SecuregraphScript script) {
        super(script);
    }

    public LazyEdge get(String edgeId) {
        Edge e = getGraph().getEdge(edgeId, getScript().getAuthorizations());
        if (e == null) {
            return null;
        }
        return new LazyEdge(getScript(), edgeId);
    }
}
