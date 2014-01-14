package com.altamiracorp.securegraph;

import com.altamiracorp.securegraph.property.MutableProperty;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class ElementBuilder<T> {
    private final List<Property> properties = new ArrayList<Property>();

    public ElementBuilder<T> setProperty(String name, Object value, Visibility visibility) {
        return setProperty(name, value, new HashMap<String, Object>(), visibility);
    }

    public ElementBuilder<T> setProperty(String name, Object value, Map<String, Object> metadata, Visibility visibility) {
        return addPropertyValue(ElementMutation.DEFAULT_ID, name, value, metadata, visibility);
    }

    public ElementBuilder<T> addPropertyValue(Object id, String name, Object value, Visibility visibility) {
        return addPropertyValue(id, name, value, new HashMap<String, Object>(), visibility);
    }

    public ElementBuilder<T> addPropertyValue(Object id, String name, Object value, Map<String, Object> metadata, Visibility visibility) {
        this.properties.add(new MutableProperty(id, name, value, metadata, visibility));
        return this;
    }

    public abstract T save();

    protected List<Property> getProperties() {
        return properties;
    }
}
