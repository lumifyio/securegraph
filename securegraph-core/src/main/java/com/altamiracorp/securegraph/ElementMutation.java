package com.altamiracorp.securegraph;

import java.util.Map;

public interface ElementMutation<T extends Element> {
    static final Object DEFAULT_ID = "";

    T save();

    ElementMutation setProperty(String name, Object value, Visibility visibility);

    ElementMutation setProperty(String name, Object value, Map<String, Object> metadata, Visibility visibility);

    ElementMutation addPropertyValue(Object id, String name, Object value, Visibility visibility);

    ElementMutation addPropertyValue(Object id, String name, Object value, Map<String, Object> metadata, Visibility visibility);
}
