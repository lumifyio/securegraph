package com.altamiracorp.securegraph.blueprints;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Vertex;

public class SecureGraphBlueprintsConvert {
    public static com.altamiracorp.securegraph.Vertex toSecureGraph(Vertex vertex) {
        if (vertex == null) {
            return null;
        }
        if (vertex instanceof SecureGraphBlueprintsVertex) {
            return ((SecureGraphBlueprintsVertex) vertex).getSecureGraphElement();
        }
        throw new IllegalArgumentException("Invalid vertex, cannot get SecureGraph vertex. " + vertex.getClass().getName());
    }

    public static com.altamiracorp.securegraph.Direction toSecureGraph(Direction direction) {
        switch (direction) {
            case OUT:
                return com.altamiracorp.securegraph.Direction.OUT;
            case IN:
                return com.altamiracorp.securegraph.Direction.IN;
            case BOTH:
                return com.altamiracorp.securegraph.Direction.BOTH;
            default:
                throw new IllegalArgumentException("Unexpected direction: " + direction);
        }
    }
}
