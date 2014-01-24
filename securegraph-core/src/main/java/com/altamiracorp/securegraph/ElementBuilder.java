package com.altamiracorp.securegraph;

import com.altamiracorp.securegraph.property.MutableProperty;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class ElementBuilder<T extends Element> implements ElementMutation<T> {
    private final List<Property> properties = new ArrayList<Property>();

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
    public ElementBuilder<T> setProperty(String name, Object value, Visibility visibility) {
        return setProperty(name, value, new HashMap<String, Object>(), visibility);
    }

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
    public ElementBuilder<T> setProperty(String name, Object value, Map<String, Object> metadata, Visibility visibility) {
        return addPropertyValue(ElementMutation.DEFAULT_ID, name, value, metadata, visibility);
    }

    /**
     * Adds or updates a property.
     *
     * @param key        The unique key given to the property allowing for multi-valued properties.
     * @param name       The name of the property.
     * @param value      The value of the property.
     * @param visibility The visibility to give this property.
     */
    public ElementBuilder<T> addPropertyValue(Object key, String name, Object value, Visibility visibility) {
        return addPropertyValue(key, name, value, new HashMap<String, Object>(), visibility);
    }

    /**
     * Adds or updates a property.
     *
     * @param key        The unique key given to the property allowing for multi-valued properties.
     * @param name       The name of the property.
     * @param value      The value of the property.
     * @param metadata   The metadata to assign to this property.
     * @param visibility The visibility to give this property.
     */
    public ElementBuilder<T> addPropertyValue(Object key, String name, Object value, Map<String, Object> metadata, Visibility visibility) {
        this.properties.add(new MutableProperty(key, name, value, metadata, visibility));
        return this;
    }

    /**
     * saves the element to the graph.
     *
     * @return either the vertex or edge just saved.
     */
    public abstract T save();

    public Iterable<Property> getProperties() {
        return properties;
    }
}
