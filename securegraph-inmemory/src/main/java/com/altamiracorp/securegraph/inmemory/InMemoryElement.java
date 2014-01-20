package com.altamiracorp.securegraph.inmemory;

import com.altamiracorp.securegraph.*;
import com.altamiracorp.securegraph.property.MutableProperty;
import com.altamiracorp.securegraph.property.StreamingPropertyValue;
import com.altamiracorp.securegraph.util.StreamUtils;

import java.io.IOException;
import java.util.List;

public abstract class InMemoryElement extends ElementBase {
    protected InMemoryElement(Graph graph, Object id, Visibility visibility, List<Property> properties) {
        super(graph, id, visibility, properties);
    }

    @Override
    public void removeProperty(String propertyId, String name) {
        Property property = removePropertyInternal(propertyId, name);
        if (property != null) {
            getGraph().removeProperty(this, property);
        }
    }

    @Override
    public void removeProperty(String name) {
        Iterable<Property> properties = removePropertyInternal(name);
        for (Property property : properties) {
            getGraph().removeProperty(this, property);
        }
    }

    @Override
    public InMemoryGraph getGraph() {
        return (InMemoryGraph) super.getGraph();
    }

    @Override
    protected void setPropertiesInternal(List<Property> properties) {
        try {
            for (Property property : properties) {
                if (property.getValue() instanceof StreamingPropertyValue) {
                    StreamingPropertyValue value = (StreamingPropertyValue) property.getValue();
                    byte[] valueData = StreamUtils.toBytes(value.getInputStream(null));
                    ((MutableProperty) property).setValue(new InMemoryStreamingPropertyValue(valueData, value.getValueType()));
                }
            }
            super.setPropertiesInternal(properties);
        } catch (IOException ex) {
            throw new SecureGraphException(ex);
        }
    }

    @Override
    protected Iterable<Property> removePropertyInternal(String name) {
        return super.removePropertyInternal(name);
    }

    @Override
    protected Property removePropertyInternal(Object propertyId, String name) {
        return super.removePropertyInternal(propertyId, name);
    }

    @Override
    public ElementMutation prepareMutation() {
        return new ElementMutationImpl() {
            @Override
            public InMemoryElement save() {
                List<Property> properties = getProperties();
                setPropertiesInternal(properties);
                getGraph().saveProperties(InMemoryElement.this, properties);
                return InMemoryElement.this;
            }
        };
    }
}
