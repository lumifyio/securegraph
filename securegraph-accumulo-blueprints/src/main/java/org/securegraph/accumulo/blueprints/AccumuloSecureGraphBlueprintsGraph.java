package org.securegraph.accumulo.blueprints;

import org.securegraph.accumulo.AccumuloGraph;
import org.securegraph.blueprints.AuthorizationsProvider;
import org.securegraph.blueprints.SecureGraphBlueprintsGraph;
import org.securegraph.blueprints.VisibilityProvider;

public class AccumuloSecureGraphBlueprintsGraph extends SecureGraphBlueprintsGraph {
    public AccumuloSecureGraphBlueprintsGraph(AccumuloGraph graph, VisibilityProvider visibilityProvider, AuthorizationsProvider authorizationsProvider) {
        super(graph, visibilityProvider, authorizationsProvider);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName().toLowerCase() + ":" + getSecureGraph().getConfiguration().getTableNamePrefix();
    }

    @Override
    public AccumuloGraph getSecureGraph() {
        return (AccumuloGraph) super.getSecureGraph();
    }
}
