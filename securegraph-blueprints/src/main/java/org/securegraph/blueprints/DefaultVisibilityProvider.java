package org.securegraph.blueprints;

import org.securegraph.Vertex;
import org.securegraph.Visibility;

import java.util.Map;

public class DefaultVisibilityProvider implements VisibilityProvider {
    private static final Visibility DEFAULT_VISIBILITY = new Visibility("");

    public DefaultVisibilityProvider(Map config) {

    }

    @Override
    public Visibility getVisibilityForEdge(Object id, Vertex outVertex, Vertex inVertex, String label) {
        return DEFAULT_VISIBILITY;
    }

    @Override
    public Visibility getVisibilityForVertex(Object id) {
        return DEFAULT_VISIBILITY;
    }

    @Override
    public Visibility getVisibilityForProperty(String propertyName, Object value) {
        return DEFAULT_VISIBILITY;
    }
}
