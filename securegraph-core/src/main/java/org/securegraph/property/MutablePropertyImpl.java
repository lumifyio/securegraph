package org.securegraph.property;

import org.securegraph.Visibility;

import java.util.HashMap;
import java.util.Map;

public class MutablePropertyImpl extends MutableProperty {
    private final String key;
    private final String name;
    private Object value;
    private Visibility visibility;
    private final Map<String, Object> metadata;

    public MutablePropertyImpl(String key, String name, Object value, Map<String, Object> metadata, Visibility visibility) {
        if (metadata == null) {
            metadata = new HashMap<String, Object>();
        }

        this.key = key;
        this.name = name;
        this.value = value;
        this.metadata = metadata;
        this.visibility = visibility;
    }

    public String getKey() {
        return key;
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

    public void setValue(Object value) {
        this.value = value;
    }

    public void setVisibility(Visibility visibility) {
        this.visibility = visibility;
    }
}
