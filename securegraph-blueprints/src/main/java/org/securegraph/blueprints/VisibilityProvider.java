package org.securegraph.blueprints;

import org.securegraph.Vertex;
import org.securegraph.Visibility;

public interface VisibilityProvider {
    Visibility getVisibilityForEdge(String id, Vertex outVertex, Vertex inVertex, String label);

    Visibility getVisibilityForVertex(String id);

    Visibility getVisibilityForProperty(String propertyName, Object value);
}
