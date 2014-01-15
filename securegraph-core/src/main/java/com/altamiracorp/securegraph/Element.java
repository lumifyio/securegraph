package com.altamiracorp.securegraph;

public interface Element {
    Object getId();

    Visibility getVisibility();

    Iterable<Property> getProperties();

    Iterable<Property> getProperties(String name);

    Iterable<Object> getPropertyValues(String name);

    Object getPropertyValue(String name, int index);

    ElementMutation prepareMutation();

    void removeProperty(String propertyId, String name);

    void removeProperty(String name);

    Graph getGraph();

    void addPropertyValue(Object id, String name, Object value, Visibility visibility);

    void setProperty(String name, Object value, Visibility visibility);
}
