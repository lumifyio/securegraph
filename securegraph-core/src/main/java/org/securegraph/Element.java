package org.securegraph;

import org.securegraph.mutation.ExistingElementMutation;

import java.util.Map;

/**
 * An element on the graph. This can be either a vertex or edge.
 * <p/>
 * Elements also contain properties. These properties are unique given their key, name, and visibility.
 * For example a property with key "key1" and name "age" could have to values, one with visibility "a" and one
 * with visibility "b".
 */
public interface Element {
    /**
     * id of the element.
     */
    Object getId();

    /**
     * the visibility of the element.
     */
    Visibility getVisibility();

    /**
     * an Iterable of all the properties on this element that you have access to based on the authorizations
     * used to retrieve the element.
     */
    Iterable<Property> getProperties();

    /**
     * Gets a property by key and name.
     *
     * @param key  the key of the property.
     * @param name the name of the property.
     * @return The property if found. null, if not found.
     */
    Property getProperty(Object key, String name);

    /**
     * Gets a property by key, name, and visibility.
     *
     * @param key        the key of the property.
     * @param name       the name of the property.
     * @param visibility The visibility of the property to get.
     * @return The property if found. null, if not found.
     */
    Property getProperty(Object key, String name, Visibility visibility);

    /**
     * Gets a property by name. This assumes a single valued property. If multiple property values exists this will only return the first one.
     *
     * @param name the name of the property.
     * @return The property if found. null, if not found.
     */
    Property getProperty(String name);

    /**
     * an Iterable of all the properties with the given name on this element that you have access to based on the authorizations
     * used to retrieve the element.
     *
     * @param name The name of the property to retrieve
     */
    Iterable<Property> getProperties(String name);

    /**
     * an Iterable of all the property values with the given name on this element that you have access to based on the authorizations
     * used to retrieve the element.
     *
     * @param name The name of the property to retrieve
     */
    Iterable<Object> getPropertyValues(String name);

    /**
     * Convenience method to retrieve the first value of the property with the given name. This method calls
     * org.securegraph.Element#getPropertyValue(java.lang.String, int) with an index of 0.
     * <p/>
     * This method makes no attempt to verify that one and only one value exists given the name.
     *
     * @param name The name of the property to retrieve
     * @return The value of the property. null, if the property was not found.
     */
    Object getPropertyValue(String name);

    /**
     * Gets the nth property value of the named property. If the named property has multiple values this method
     * provides an easy way to get the value by index.
     * <p/>
     * This method is a convenience method and calls org.securegraph.Element#getPropertyValues(java.lang.String)
     * and iterates over that list until the nth value.
     * <p/>
     * This method assumes the property values are retrieved in a deterministic order.
     *
     * @param name  The name of the property to retrieve.
     * @param index The zero based index into the values.
     * @return The value of the property. null, if the property doesn't exist or doesn't have that many values.
     */
    Object getPropertyValue(String name, int index);

    /**
     * Prepares a mutation to allow changing multiple property values at the same time. This method is similar to
     * org.securegraph.Graph#prepareVertex(org.securegraph.Visibility, org.securegraph.Authorizations)
     * in that it allows multiple properties to be changed and saved in a single mutation.
     *
     * @return The mutation builder.
     */
    ExistingElementMutation prepareMutation();

    /**
     * Removes a property given it's key and name from the element. Only properties which you have access to can be removed using
     * this method.
     *
     * @param key  The property key.
     * @param name The property name.
     */
    void removeProperty(String key, String name);

    /**
     * Removes all properties with the given name that you have access to. Only properties which you have access to will be removed.
     *
     * @param name The name of the property to remove.
     */
    void removeProperty(String name);

    /**
     * Gets the graph that this element belongs to.
     */
    Graph getGraph();

    /**
     * Adds or updates a property.
     *
     * @param key        The unique key given to the property allowing for multi-valued properties.
     * @param name       The name of the property.
     * @param value      The value of the property.
     * @param visibility The visibility to give this property.
     */
    void addPropertyValue(String key, String name, Object value, Visibility visibility);

    /**
     * Adds or updates a property.
     *
     * @param key        The unique key given to the property allowing for multi-valued properties.
     * @param name       The name of the property.
     * @param value      The value of the property.
     * @param metadata   The metadata to assign to this property.
     * @param visibility The visibility to give this property.
     */
    void addPropertyValue(String key, String name, Object value, Map<String, Object> metadata, Visibility visibility);

    /**
     * Sets or updates a property value. The property key will be set to a constant. This is a convenience method
     * which allows treating the multi-valued nature of properties as only containing a single value. Care must be
     * taken when using this method because properties are not only uniquely identified by just key and name but also
     * visibility so adding properties with the same name and different visibility strings is still permitted.
     *
     * @param name       The name of the property.
     * @param value      The value of the property.
     * @param visibility The visibility to give this property.
     */
    void setProperty(String name, Object value, Visibility visibility);

    /**
     * Sets or updates a property value. The property key will be set to a constant. This is a convenience method
     * which allows treating the multi-valued nature of properties as only containing a single value. Care must be
     * taken when using this method because properties are not only uniquely identified by just key and name but also
     * visibility so adding properties with the same name and different visibility strings is still permitted.
     *
     * @param name       The name of the property.
     * @param value      The value of the property.
     * @param metadata   The metadata to assign to this property.
     * @param visibility The visibility to give this property.
     */
    void setProperty(String name, Object value, Map<String, Object> metadata, Visibility visibility);
}
