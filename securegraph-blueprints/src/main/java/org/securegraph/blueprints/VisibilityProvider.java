package org.securegraph.blueprints;

import org.securegraph.Vertex;
import org.securegraph.Visibility;

public interface VisibilityProvider {
    Visibility getVisibilityForEdge(Object id, Vertex outVertex, Vertex inVertex, String label);

    Visibility getVisibilityForVertex(Object id);

    Visibility getVisibilityForProperty(String propertyName, Object value);
}
