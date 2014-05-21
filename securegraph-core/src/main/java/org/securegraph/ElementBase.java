package org.securegraph;

import org.securegraph.mutation.ExistingElementMutation;
import org.securegraph.property.PropertyValue;
import org.securegraph.util.ConvertingIterable;
import org.securegraph.util.FilterIterable;

import java.util.*;

public abstract class ElementBase<T extends Element> implements Element {
    private final Graph graph;
    private final Object id;
    private Visibility visibility;

    private final TreeSet<Property> properties;

    protected ElementBase(Graph graph, Object id, Visibility visibility, Iterable<Property> properties) {
        this.graph = graph;
        this.id = id;
        this.visibility = visibility;
        this.properties = new TreeSet<Property>();
        updatePropertiesInternal(properties);
    }

    @Override
    public Iterable<Object> getPropertyValues(final String name) {
        return new ConvertingIterable<Property, Object>(getProperties(name)) {
            @Override
            protected Object convert(Property p) {
                return p.getValue();
            }
        };
    }

    @Override
    public Iterable<Object> getPropertyValues(Object key, String name) {
        return new ConvertingIterable<Property, Object>(getProperties(key, name)) {
            @Override
            protected Object convert(Property p) {
                return p.getValue();
            }
        };
    }

    @Override
    public Property getProperty(Object key, String name, Visibility visibility) {
        for (Property p : getProperties()) {
            if (!p.getKey().equals(key)) {
                continue;
            }
            if (!p.getName().equals(name)) {
                continue;
            }
            if (visibility == null) {
                return p;
            }
            if (!visibility.equals(p.getVisibility())) {
                continue;
            }
            return p;
        }
        return null;
    }

    @Override
    public Property getProperty(Object key, String name) {
        return getProperty(key, name, null);
    }

    @Override
    public Property getProperty(String name) {
        Iterator<Property> propertiesWithName = getProperties(name).iterator();
        if (propertiesWithName.hasNext()) {
            return propertiesWithName.next();
        }
        return null;
    }

    @Override
    public Object getPropertyValue(String name) {
        return getPropertyValue(name, 0);
    }

    @Override
    public Object getPropertyValue(String name, int index) {
        Iterator<Object> values = getPropertyValues(name).iterator();
        while (values.hasNext() && index >= 0) {
            Object v = values.next();
            if (index == 0) {
                return v;
            }
            index--;
        }
        return null;
    }

    @Override
    public Object getPropertyValue(Object key, String name, int index) {
        Iterator<Object> values = getPropertyValues(key, name).iterator();
        while (values.hasNext() && index >= 0) {
            Object v = values.next();
            if (index == 0) {
                return v;
            }
            index--;
        }
        return null;
    }

    @Override
    public Object getPropertyValue(Object key, String name) {
        return getPropertyValue(key, name, 0);
    }

    @Override
    public Object getId() {
        return this.id;
    }

    @Override
    public Visibility getVisibility() {
        return this.visibility;
    }

    protected void setVisibility(Visibility visibility) {
        this.visibility = visibility;
    }

    @Override
    public Iterable<Property> getProperties() {
        return this.properties;
    }

    @Override
    public Iterable<Property> getProperties(final String name) {
        return new FilterIterable<Property>(getProperties()) {
            @Override
            protected boolean isIncluded(Property property) {
                return property.getName().equals(name);
            }
        };
    }

    @Override
    public Iterable<Property> getProperties(final Object key, final String name) {
        return new FilterIterable<Property>(getProperties()) {
            @Override
            protected boolean isIncluded(Property property) {

                return property.getName().equals(name) && property.getKey().equals(key);
            }
        };
    }

    // this method differs setProperties in that it only updates the in memory representation of the properties
    protected void updatePropertiesInternal(Iterable<Property> properties) {
        for (Property property : properties) {
            if (property.getKey() == null) {
                throw new IllegalArgumentException("key is required for property");
            }
            Object propertyValue = property.getValue();
            if (propertyValue instanceof PropertyValue && !((PropertyValue) propertyValue).isStore()) {
                continue;
            }
            this.properties.remove(property);
            this.properties.add(property);
        }
    }

    protected Property removePropertyInternal(Object key, String name) {
        Property property = getProperty(key, name);
        if (property != null) {
            this.properties.remove(property);
        }
        return property;
    }

    protected Iterable<Property> removePropertyInternal(String name) {
        List<Property> removedProperties = new ArrayList<Property>();
        for (Property p : this.properties) {
            if (p.getName().equals(name)) {
                removedProperties.add(p);
            }
        }

        for (Property p : removedProperties) {
            this.properties.remove(p);
        }

        return removedProperties;
    }

    public Graph getGraph() {
        return graph;
    }

    @Override
    public int hashCode() {
        return getId().hashCode();
    }

    @Override
    public String toString() {
        if (this instanceof Edge) {
            return getId() + ":[" + ((Edge) this).getVertexId(Direction.OUT) + "->" + ((Edge) this).getVertexId(Direction.IN) + "]";
        }
        return getId().toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Element) {
            Element objElem = (Element) obj;
            return getId().equals(objElem.getId());
        }
        return super.equals(obj);
    }

    @Override
    public abstract ExistingElementMutation<T> prepareMutation();

    @Override
    public abstract void removeProperty(String key, String name, Authorizations authorizations);

    @Override
    public void addPropertyValue(String key, String name, Object value, Visibility visibility, Authorizations authorizations) {
        prepareMutation().addPropertyValue(key, name, value, visibility).save(authorizations);
    }

    @Override
    public void addPropertyValue(String key, String name, Object value, Map<String, Object> metadata, Visibility visibility, Authorizations authorizations) {
        prepareMutation().addPropertyValue(key, name, value, metadata, visibility).save(authorizations);
    }

    @Override
    public void setProperty(String name, Object value, Visibility visibility, Authorizations authorizations) {
        prepareMutation().setProperty(name, value, visibility).save(authorizations);
    }

    @Override
    public void setProperty(String name, Object value, Map<String, Object> metadata, Visibility visibility, Authorizations authorizations) {
        prepareMutation().setProperty(name, value, metadata, visibility).save(authorizations);
    }

    @Override
    public abstract void removeProperty(String name, Authorizations authorizations);
}
