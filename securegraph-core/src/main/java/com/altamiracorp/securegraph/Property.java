package com.altamiracorp.securegraph;

import java.util.Map;

public abstract class Property {
    private final Object id;
    private final String name;
    private Object value;
    private final Visibility visibility;
    private final Map<String, Object> metadata;

    protected Property(Object id, String name, Object value, Map<String, Object> metadata, Visibility visibility) {
        this.id = id;
        this.name = name;
        this.value = value;
        this.metadata = metadata;
        this.visibility = visibility;
    }

    protected void setValue(Object value) {
        this.value = value;
    }

    public Object getId() {
        return id;
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
