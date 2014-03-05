package com.altamiracorp.securegraph.inmemory;

import com.altamiracorp.securegraph.*;
import com.altamiracorp.securegraph.mutation.ExistingElementMutationImpl;
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
    protected void updatePropertiesInternal(Iterable<Property> properties) {
        try {
            for (Property property : properties) {
                if (property.getValue() instanceof StreamingPropertyValue) {
                    StreamingPropertyValue value = (StreamingPropertyValue) property.getValue();
                    byte[] valueData = StreamUtils.toBytes(value.getInputStream());
                    ((MutableProperty) property).setValue(new InMemoryStreamingPropertyValue(valueData, value.getValueType()));
                }
            }
            super.updatePropertiesInternal(properties);
        } catch (IOException ex) {
            throw new SecureGraphException(ex);
        }
    }

    @Override
    protected Iterable<Property> removePropertyInternal(String name) {
        return super.removePropertyInternal(name);
    }

    @Override
    protected Property removePropertyInternal(Object key, String name) {
        return super.removePropertyInternal(key, name);
    }

    protected <TElement extends Element> void saveExistingElementMutation(ExistingElementMutationImpl<TElement> mutation) {
        Iterable<Property> properties = mutation.getProperties();
        updatePropertiesInternal(properties);
        getGraph().saveProperties(mutation.getElement(), properties);

        if (mutation.getElement() instanceof Edge) {
            if (mutation.getNewElementVisibility() != null) {
                getGraph().alterEdgeVisibility(mutation.getElement().getId(), mutation.getNewElementVisibility());
            }
            getGraph().alterEdgePropertyVisibilities(mutation.getElement().getId(), mutation.getAlterPropertyVisibilities());
            getGraph().alterEdgePropertyMetadata(mutation.getElement().getId(), mutation.getAlterPropertyMetadatas());
        } else if (mutation.getElement() instanceof Vertex) {
            if (mutation.getNewElementVisibility() != null) {
                getGraph().alterVertexVisibility(mutation.getElement().getId(), mutation.getNewElementVisibility());
            }
            getGraph().alterVertexPropertyVisibilities(mutation.getElement().getId(), mutation.getAlterPropertyVisibilities());
            getGraph().alterVertexPropertyMetadata(mutation.getElement().getId(), mutation.getAlterPropertyMetadatas());
        } else {
            throw new IllegalStateException("Unexpected element type: " + mutation.getElement());
        }
    }

    void setVisibilityInternal(Visibility visibility) {
        super.setVisibility(visibility);
    }
}
