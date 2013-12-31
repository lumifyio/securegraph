package com.altamiracorp.securegraph;

import java.util.HashMap;
import java.util.Map;

public class Property {
    private final String name;
    private final Object value;
    private final Visibility visibility;
    private final Map<String, Object> metadata;

    public Property(String name, Object value, Visibility visibility) {
        this(name, value, visibility, new HashMap<String, Object>());
    }

    public Property(String name, Object value, Visibility visibility, Map<String, Object> metadata) {
        this.name = name;
        this.value = value;
        this.visibility = visibility;
        this.metadata = metadata;
    }

    public String getName() {
        return name;
    }

    public Object getValue() {
        return value;
    }

    public Visibility getVisibility() {
        return visibility;
    }

    public Map<String, Object> getMetadata() {
        return metadata;
    }
}
