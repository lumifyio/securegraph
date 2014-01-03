package com.altamiracorp.securegraph.accumulo.blueprints;

import com.altamiracorp.securegraph.accumulo.AccumuloGraph;
import com.altamiracorp.securegraph.blueprints.AuthorizationsProvider;
import com.altamiracorp.securegraph.blueprints.SecureGraphBlueprintsGraph;
import com.altamiracorp.securegraph.blueprints.VisibilityProvider;

public class AccumuloSecureGraphBlueprintsGraph extends SecureGraphBlueprintsGraph {
    public AccumuloSecureGraphBlueprintsGraph(AccumuloGraph graph, VisibilityProvider visibilityProvider, AuthorizationsProvider authorizationsProvider) {
        super(graph, visibilityProvider, authorizationsProvider);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName().toLowerCase() + ":" + getSecureGraph().getConfiguration().getTableName();
    }

    @Override
    public AccumuloGraph getSecureGraph() {
        return (AccumuloGraph) super.getSecureGraph();
    }
}
