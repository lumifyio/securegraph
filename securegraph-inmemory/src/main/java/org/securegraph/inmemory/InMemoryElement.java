package org.securegraph.inmemory;

import org.securegraph.*;
import org.securegraph.mutation.ExistingElementMutationImpl;
import org.securegraph.property.MutableProperty;
import org.securegraph.property.StreamingPropertyValue;
import org.securegraph.util.StreamUtils;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public abstract class InMemoryElement<T extends Element> extends ElementBase<T> {
    private Set<HiddenProperty> hiddenProperties = new HashSet<HiddenProperty>();

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
    public void markPropertyHidden(String key, String name, Visibility propertyVisibility, Visibility visibility, Authorizations authorizations) {
        Iterable<Property> properties = getProperties(key, name);
        for (Property property : properties) {
            if (property.getVisibility().equals(propertyVisibility)) {
                getGraph().markPropertyHidden(this, property, visibility, authorizations);
                return;
            }
        }
        throw new IllegalArgumentException("Could not find property " + key + " : " + name + " : " + propertyVisibility);
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
        this.hiddenProperties.add(new HiddenProperty(property, visibility));
    }

    boolean isPropertyHidden(Property property, Authorizations authorizations) {
        for (HiddenProperty hiddenProperty : this.hiddenProperties) {
            if (hiddenProperty.matches(property, authorizations)) {
                return true;
            }
        }
        return false;
    }

    protected static class HiddenProperty {
        private final String key;
        private final String name;
        private final Visibility propertyVisibility;
        private final Visibility hiddenVisibility;

        public HiddenProperty(Property property, Visibility hiddenVisibility) {
            this.key = property.getKey();
            this.name = property.getName();
            this.propertyVisibility = property.getVisibility();
            this.hiddenVisibility = hiddenVisibility;
        }

        public boolean matches(Property prop, Authorizations authorizations) {
            if (!prop.getName().equals(this.name)
                    || !prop.getKey().equals(this.key)
                    || !prop.getVisibility().equals(this.propertyVisibility)) {
                return false;
            }
            return authorizations.canRead(hiddenVisibility);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            HiddenProperty that = (HiddenProperty) o;

            if (key != null ? !key.equals(that.key) : that.key != null) {
                return false;
            }
            if (name != null ? !name.equals(that.name) : that.name != null) {
                return false;
            }
            if (propertyVisibility != null ? !propertyVisibility.equals(that.propertyVisibility) : that.propertyVisibility != null) {
                return false;
            }
            if (hiddenVisibility != null ? !hiddenVisibility.equals(that.hiddenVisibility) : that.hiddenVisibility != null) {
                return false;
            }

            return true;
        }

        @Override
        public int hashCode() {
            int result = key != null ? key.hashCode() : 0;
            result = 31 * result + (name != null ? name.hashCode() : 0);
            return result;
        }
    }
}
