package com.altamiracorp.securegraph.blueprints;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;

public class SecureGraphBlueprintsEdge extends SecureGraphBlueprintsElement implements Edge {
    protected SecureGraphBlueprintsEdge(SecureGraphBlueprintsGraph graph, com.altamiracorp.securegraph.Edge edge) {
        super(graph, edge);
    }

    public static Edge create(SecureGraphBlueprintsGraph graph, com.altamiracorp.securegraph.Edge edge) {
        if (edge == null) {
            return null;
        }
        return new SecureGraphBlueprintsEdge(graph, edge);
    }

    @Override
    public Vertex getVertex(Direction direction) throws IllegalArgumentException {
        throw new RuntimeException("not implemented");
    }

    @Override
    public String getLabel() {
        return getSecureGraphElement().getLabel();
    }

    @Override
    public com.altamiracorp.securegraph.Edge getSecureGraphElement() {
        return (com.altamiracorp.securegraph.Edge) super.getSecureGraphElement();
    }
}
