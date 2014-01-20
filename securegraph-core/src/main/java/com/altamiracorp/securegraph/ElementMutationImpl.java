package com.altamiracorp.securegraph;

import com.altamiracorp.securegraph.property.MutableProperty;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class ElementMutationImpl<T extends Element> implements ElementMutation<T> {
    private final List<Property> properties = new ArrayList<Property>();

    public abstract T save();

    public ElementMutation setProperty(String name, Object value, Visibility visibility) {
        return setProperty(name, value, new HashMap<String, Object>(), visibility);
    }

    public ElementMutation setProperty(String name, Object value, Map<String, Object> metadata, Visibility visibility) {
        return addPropertyValue(DEFAULT_ID, name, value, metadata, visibility);
    }

    public ElementMutation addPropertyValue(Object id, String name, Object value, Visibility visibility) {
        return addPropertyValue(id, name, value, new HashMap<String, Object>(), visibility);
    }

    public ElementMutation addPropertyValue(Object id, String name, Object value, Map<String, Object> metadata, Visibility visibility) {
        properties.add(new MutableProperty(id, name, value, metadata, visibility));
        return this;
    }

    protected List<Property> getProperties() {
        return properties;
    }
}
