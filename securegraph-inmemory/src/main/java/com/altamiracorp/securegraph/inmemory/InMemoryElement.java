package com.altamiracorp.securegraph.inmemory;

import com.altamiracorp.securegraph.*;
import com.altamiracorp.securegraph.property.PropertyBase;
import com.altamiracorp.securegraph.property.StreamingPropertyValue;
import com.altamiracorp.securegraph.util.StreamUtils;

import java.io.IOException;

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
    protected void setPropertiesInternal(Property[] properties) {
        try {
            for (Property property : properties) {
                if (property.getValue() instanceof StreamingPropertyValue) {
                    StreamingPropertyValue value = (StreamingPropertyValue) property.getValue();
                    byte[] valueData = StreamUtils.toBytes(value.getInputStream(null));
                    ((PropertyBase) property).setValue(new InMemoryStreamingPropertyValue(valueData, value.getValueType()));
                }
            }
            super.setPropertiesInternal(properties);
        } catch (IOException ex) {
            throw new SecureGraphException(ex);
        }
    }

    @Override
    protected Property removePropertyInternal(Object propertyId, String name) {
        return super.removePropertyInternal(propertyId, name);
    }
}
