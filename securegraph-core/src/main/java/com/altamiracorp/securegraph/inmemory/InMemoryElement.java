package com.altamiracorp.securegraph.inmemory;

import com.altamiracorp.securegraph.ElementBase;
import com.altamiracorp.securegraph.Graph;
import com.altamiracorp.securegraph.Property;
import com.altamiracorp.securegraph.Visibility;

public abstract class InMemoryElement extends ElementBase {
    protected InMemoryElement(Graph graph, Object id, Visibility visibility, Property[] properties) {
        super(graph, id, visibility, properties);
    }

    @Override
    public void setProperties(Property... properties) {
        setPropertiesInternal(properties);
        getGraph().saveProperties(this, properties);
    }

    @Override
    public void removeProperty(String propertyId, String name) {
        Property property = removePropertyInternal(propertyId, name);
        if (property != null) {
            getGraph().removeProperty(this, property);
        }
    }

    @Override
    public InMemoryGraph getGraph() {
        return (InMemoryGraph) super.getGraph();
    }

    @Override
    public void setPropertiesInternal(Property[] properties) {
        super.setPropertiesInternal(properties);
    }

    @Override
    public Property removePropertyInternal(String propertyId, String name) {
        return super.removePropertyInternal(propertyId, name);
    }
}
