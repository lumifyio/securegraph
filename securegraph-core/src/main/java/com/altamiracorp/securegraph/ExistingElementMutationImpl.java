package com.altamiracorp.securegraph;

import com.altamiracorp.securegraph.property.MutableProperty;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class ExistingElementMutationImpl<T extends Element> implements ElementMutation<T>, ExistingElementMutation<T> {
    private final List<Property> properties = new ArrayList<Property>();
    private final T element;

    public ExistingElementMutationImpl(T element) {
        this.element = element;
    }

    public abstract T save();

    public ElementMutation<T> setProperty(String name, Object value, Visibility visibility) {
        return setProperty(name, value, new HashMap<String, Object>(), visibility);
    }

    public ElementMutation<T> setProperty(String name, Object value, Map<String, Object> metadata, Visibility visibility) {
        return addPropertyValue(DEFAULT_ID, name, value, metadata, visibility);
    }

    public ElementMutation<T> addPropertyValue(String key, String name, Object value, Visibility visibility) {
        return addPropertyValue(key, name, value, new HashMap<String, Object>(), visibility);
    }

    public ElementMutation<T> addPropertyValue(String key, String name, Object value, Map<String, Object> metadata, Visibility visibility) {
        properties.add(new MutableProperty(key, name, value, metadata, visibility));
        return this;
    }

    public Iterable<Property> getProperties() {
        return properties;
    }

    @Override
    public T getElement() {
        return element;
    }
}
