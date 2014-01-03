package com.altamiracorp.securegraph;

import java.util.HashMap;
import java.util.Map;

public class Property {
    private String id;
    private final String name;
    private final Object value;
    private final Visibility visibility;
    private final Map<String, Object> metadata;

    public Property(String name, Object value, Visibility visibility) {
        this(null, name, value, visibility);
    }

    public Property(String id, String name, Object value, Visibility visibility) {
        this(id, name, value, visibility, new HashMap<String, Object>());
    }

    public Property(String name, Object value, Visibility visibility, Map<String, Object> metadata) {
        this(null, name, value, visibility, metadata);
    }

    public Property(String id, String name, Object value, Visibility visibility, Map<String, Object> metadata) {
        this.id = id;
        this.name = name;
        this.value = value;
        this.visibility = visibility;
        this.metadata = metadata;
    }

    public String getId() {
        return id;
    }

    // TODO this seems a little dirty to allow setting the id
    public void setId(String id) {
        this.id = id;
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
