package com.altamiracorp.securegraph.blueprints;

import com.altamiracorp.securegraph.Vertex;
import com.altamiracorp.securegraph.Visibility;

public interface VisibilityProvider {
    Visibility getVisibilityForEdge(Object id, Vertex outVertex, Vertex inVertex, String label);

    Visibility getVisibilityForVertex(Object id);

    Visibility getVisibilityForProperty(String propertyName, Object value);
}
