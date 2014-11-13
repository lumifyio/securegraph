package org.securegraph.inmemory;

import org.securegraph.*;
import org.securegraph.mutation.ExistingElementMutationImpl;
import org.securegraph.property.MutableProperty;
import org.securegraph.property.StreamingPropertyValue;
import org.securegraph.util.StreamUtils;

import java.io.IOException;

public abstract class InMemoryElement<T extends Element> extends ElementBase<T> {
    protected InMemoryElement(Graph graph, String id, Visibility visibility, Iterable<Property> properties, Iterable<Visibility> hiddenVisibilities, Authorizations authorizations) {
        super(graph, id, visibility, properties, hiddenVisibilities, authorizations);
    }

    @Override
    public void removeProperty(String key, String name, Authorizations authorizations) {
        Property property = removePropertyInternal(key, name);
        if (property != null) {
            getGraph().removeProperty(this, property, authorizations);
        }
    }

    @Override
    public void removeProperty(String name, Authorizations authorizations) {
        Iterable<Property> properties = removePropertyInternal(name);
        for (Property property : properties) {
            getGraph().removeProperty(this, property, authorizations);
        }
    }

    @Override
    public void markPropertyHidden(Property property, Visibility visibility, Authorizations authorizations) {
        getGraph().markPropertyHidden(this, property, visibility, authorizations);
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
    protected Property removePropertyInternal(String key, String name) {
        return super.removePropertyInternal(key, name);
    }

    protected <TElement extends Element> void saveExistingElementMutation(ExistingElementMutationImpl<TElement> mutation, Authorizations authorizations) {
        Iterable<Property> properties = mutation.getProperties();
        updatePropertiesInternal(properties);
        getGraph().saveProperties(mutation.getElement(), properties, mutation.getIndexHint(), authorizations);

        if (mutation.getElement() instanceof Edge) {
            if (mutation.getNewElementVisibility() != null) {
                getGraph().alterEdgeVisibility(mutation.getElement().getId(), mutation.getNewElementVisibility());
            }
            getGraph().alterEdgePropertyVisibilities(mutation.getElement().getId(), mutation.getAlterPropertyVisibilities(), authorizations);
            getGraph().alterEdgePropertyMetadata(mutation.getElement().getId(), mutation.getAlterPropertyMetadatas());
        } else if (mutation.getElement() instanceof Vertex) {
            if (mutation.getNewElementVisibility() != null) {
                getGraph().alterVertexVisibility(mutation.getElement().getId(), mutation.getNewElementVisibility());
            }
            getGraph().alterVertexPropertyVisibilities(mutation.getElement().getId(), mutation.getAlterPropertyVisibilities(), authorizations);
            getGraph().alterVertexPropertyMetadata(mutation.getElement().getId(), mutation.getAlterPropertyMetadatas());
        } else {
            throw new IllegalStateException("Unexpected element type: " + mutation.getElement());
        }
    }

    void setVisibilityInternal(Visibility visibility) {
        super.setVisibility(visibility);
    }

    public void addHiddenVisibility(Visibility visibility) {
        super.addHiddenVisibility(visibility);
    }

    public boolean canRead(Authorizations authorizations) {
        // this is just a shortcut so that we don't need to construct evaluators and visibility objects to check for an empty string.
        if (getVisibility().getVisibilityString().length() > 0 && !authorizations.canRead(getVisibility())) {
            return false;
        }

        return true;
    }

    void markPropertyHiddenInternal(Property property, Visibility visibility, Authorizations authorizations) {
        if (property instanceof MutableProperty) {
            ((MutableProperty) property).addHiddenVisibility(visibility);
        } else {
            throw new SecureGraphException("Could not mark property hidden. Must be of type " + MutableProperty.class.getName());
        }
    }
}
