package org.securegraph.blueprints;

import com.tinkerpop.blueprints.Element;
import org.securegraph.Authorizations;
import org.securegraph.Property;
import org.securegraph.Visibility;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public abstract class SecureGraphBlueprintsElement implements Element {
    private static final String DEFAULT_PROPERTY_ID = "";
    private final org.securegraph.Element element;
    private final Authorizations authorizations;
    private final SecureGraphBlueprintsGraph graph;

    protected SecureGraphBlueprintsElement(SecureGraphBlueprintsGraph graph, org.securegraph.Element element, Authorizations authorizations) {
        this.graph = graph;
        this.element = element;
        this.authorizations = authorizations;
    }

    @Override
    public <T> T getProperty(String key) {
        Iterator<Object> values = getSecureGraphElement().getPropertyValues(key).iterator();
        if (values.hasNext()) {
            return (T) values.next();
        }
        return null;
    }

    @Override
    public Set<String> getPropertyKeys() {
        Set<String> propertyKeys = new HashSet<String>();
        for (Property property : getSecureGraphElement().getProperties()) {
            propertyKeys.add(property.getName());
        }
        return propertyKeys;
    }

    @Override
    public void setProperty(String propertyName, Object value) {
        if (value == null) {
            throw new IllegalArgumentException("value cannot be null.");
        }
        if (propertyName == null) {
            throw new IllegalArgumentException("Property Name cannot be null.");
        }
        if ("id".equals(propertyName)) {
            throw new IllegalArgumentException("Property Name cannot be \"id\"");
        }
        if ("".equals(propertyName)) {
            throw new IllegalArgumentException("Property Name cannot be empty.");
        }
        Visibility visibility = getGraph().getVisibilityProvider().getVisibilityForProperty(propertyName, value);
        getSecureGraphElement().setProperty(propertyName, value, visibility, authorizations);
    }

    @Override
    public <T> T removeProperty(String key) {
        T old = getProperty(key);
        getSecureGraphElement().removeProperty(DEFAULT_PROPERTY_ID, key, authorizations);
        return old;
    }

    @Override
    public abstract void remove();

    @Override
    public Object getId() {
        return getSecureGraphElement().getId();
    }

    public SecureGraphBlueprintsGraph getGraph() {
        return graph;
    }

    public org.securegraph.Element getSecureGraphElement() {
        return element;
    }

    @Override
    public int hashCode() {
        return getSecureGraphElement().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof SecureGraphBlueprintsElement) {
            return getSecureGraphElement().equals(((SecureGraphBlueprintsElement) obj).getSecureGraphElement());
        }
        return super.equals(obj);
    }

    @Override
    public String toString() {
        return getSecureGraphElement().toString();
    }
}
