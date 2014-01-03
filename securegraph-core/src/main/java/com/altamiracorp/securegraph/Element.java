package com.altamiracorp.securegraph;

public interface Element {
    Object getId();

    Visibility getVisibility();

    Iterable<Property> getProperties();

    Iterable<Property> getProperties(String name);

    Iterable<Object> getPropertyValues(String name);

    void setProperties(Property... properties);

    void removeProperty(String propertyId, String name);
}
