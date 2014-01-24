package com.altamiracorp.securegraph.inmemory;

import com.altamiracorp.securegraph.*;
import com.altamiracorp.securegraph.property.MutableProperty;
import com.altamiracorp.securegraph.property.StreamingPropertyValue;
import com.altamiracorp.securegraph.util.StreamUtils;

import java.io.IOException;

public abstract class InMemoryElement extends ElementBase {
    protected InMemoryElement(Graph graph, Object id, Visibility visibility, Iterable<Property> properties) {
        super(graph, id, visibility, properties);
    }

    @Override
    public void removeProperty(String key, String name) {
        Property property = removePropertyInternal(key, name);
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
    protected void setPropertiesInternal(Iterable<Property> properties) {
        try {
            for (Property property : properties) {
                if (property.getValue() instanceof StreamingPropertyValue) {
                    StreamingPropertyValue value = (StreamingPropertyValue) property.getValue();
                    byte[] valueData = StreamUtils.toBytes(value.getInputStream());
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
        return new ExistingElementMutationImpl<InMemoryElement>(this) {
            @Override
            public InMemoryElement save() {
                Iterable<Property> properties = getProperties();
                setPropertiesInternal(properties);
                getGraph().saveProperties(getElement(), properties);
                return getElement();
            }
        };
    }
}
