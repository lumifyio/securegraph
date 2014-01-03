package com.altamiracorp.securegraph.accumulo.blueprints;

import com.altamiracorp.securegraph.accumulo.AccumuloGraph;
import com.altamiracorp.securegraph.blueprints.SecureGraphBlueprintsGraph;

public class AccumuloSecureGraphBlueprintsGraph extends SecureGraphBlueprintsGraph {
    public AccumuloSecureGraphBlueprintsGraph(AccumuloGraph graph) {
        super(graph);
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
