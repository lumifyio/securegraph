package com.altamiracorp.securegraph;

import java.util.Map;

public interface ElementMutation<T extends Element> {
    static final Object DEFAULT_ID = "";

    T save();

    ElementMutation<T> setProperty(String name, Object value, Visibility visibility);

    ElementMutation<T> setProperty(String name, Object value, Map<String, Object> metadata, Visibility visibility);

    ElementMutation<T> addPropertyValue(Object id, String name, Object value, Visibility visibility);

    ElementMutation<T> addPropertyValue(Object id, String name, Object value, Map<String, Object> metadata, Visibility visibility);
}
