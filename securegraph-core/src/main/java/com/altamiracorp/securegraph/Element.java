package com.altamiracorp.securegraph;

public interface Element {
    Object getId();

    Visibility getVisibility();

    Iterable<Property> getProperties();

    Iterable<Property> getProperties(String name);

    Iterable<Object> getPropertyValues(String name);

    /**
     * Adds or updates existing properties.
     *
     * @param properties properties to add or update.
     */
    void setProperties(Property... properties);

    /**
     * Adds properties if a property with the given name already exists it will add a duplicate property with a different value.
     *
     * @param properties properties to add.
     */
    void addProperties(Property... properties);
}
