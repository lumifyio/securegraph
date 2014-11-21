package org.securegraph;

import org.securegraph.mutation.ExistingElementMutation;
import org.securegraph.property.PropertyValue;
import org.securegraph.util.ConvertingIterable;
import org.securegraph.util.FilterIterable;

import java.util.*;
import java.util.concurrent.ConcurrentSkipListSet;

public abstract class ElementBase<T extends Element> implements Element {
    private final Graph graph;
    private final String id;
    private Visibility visibility;
    private Set<Visibility> hiddenVisibilities = new HashSet<Visibility>();

    private final ConcurrentSkipListSet<Property> properties;
    private final Authorizations authorizations;

    protected ElementBase(Graph graph, String id, Visibility visibility, Iterable<Property> properties, Iterable<Visibility> hiddenVisibilities, Authorizations authorizations) {
        this.graph = graph;
        this.id = id;
        this.visibility = visibility;
        this.properties = new ConcurrentSkipListSet<Property>();
        this.authorizations = authorizations;
        if (hiddenVisibilities != null) {
            for (Visibility v : hiddenVisibilities) {
                this.hiddenVisibilities.add(v);
            }
        }
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
    public Iterable<Object> getPropertyValues(String key, String name) {
        return new ConvertingIterable<Property, Object>(getProperties(key, name)) {
            @Override
            protected Object convert(Property p) {
                return p.getValue();
            }
        };
    }

    @Override
    public Property getProperty(String key, String name, Visibility visibility) {
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
    public Property getProperty(String key, String name) {
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
    public Object getPropertyValue(String key, String name, int index) {
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
    public Object getPropertyValue(String key, String name) {
        return getPropertyValue(key, name, 0);
    }

    @Override
    public String getId() {
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
    public Iterable<Property> getProperties(final String key, final String name) {
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

    protected Property removePropertyInternal(String key, String name) {
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
        return getId();
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
    public void markPropertyHidden(String key, String name, Visibility propertyVisibility, Visibility visibility, Authorizations authorizations) {
        Iterable<Property> properties = getProperties(key, name);
        for (Property property : properties) {
            if (property.getVisibility().equals(propertyVisibility)) {
                markPropertyHidden(property, visibility, authorizations);
                return;
            }
        }
        throw new IllegalArgumentException("Could not find property " + key + " : " + name + " : " + propertyVisibility);
    }

    @Override
    public void markPropertyVisible(String key, String name, Visibility propertyVisibility, Visibility visibility, Authorizations authorizations) {
        Iterable<Property> properties = getProperties(key, name);
        for (Property property : properties) {
            if (property.getVisibility().equals(propertyVisibility)) {
                markPropertyVisible(property, visibility, authorizations);
                return;
            }
        }
        throw new IllegalArgumentException("Could not find property " + key + " : " + name + " : " + propertyVisibility);
    }

    @Override
    public abstract void removeProperty(String name, Authorizations authorizations);

    @Override
    public Authorizations getAuthorizations() {
        return authorizations;
    }

    @Override
    public void mergeProperties(Element element) {
        for (Property property : element.getProperties()) {
            this.properties.remove(property);
            this.properties.add(property);
        }
    }

    public Iterable<Visibility> getHiddenVisibilities() {
        return hiddenVisibilities;
    }

    @Override
    public boolean isHidden(Authorizations authorizations) {
        for (Visibility visibility : getHiddenVisibilities()) {
            if (authorizations.canRead(visibility)) {
                return true;
            }
        }
        return false;
    }

    protected void addHiddenVisibility(Visibility visibility) {
        this.hiddenVisibilities.add(visibility);
    }

    protected void removeHiddenVisibility(Visibility visibility) {
        this.hiddenVisibilities.remove(visibility);
    }
}
