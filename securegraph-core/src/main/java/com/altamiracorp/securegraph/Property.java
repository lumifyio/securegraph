package com.altamiracorp.securegraph;

import java.util.Map;

public abstract class Property implements Comparable<Property> {
    private final String key;
    private final String name;
    private Object value;
    private final Visibility visibility;
    private final Map<String, Object> metadata;

    protected Property(String key, String name, Object value, Map<String, Object> metadata, Visibility visibility) {
        this.key = key;
        this.name = name;
        this.value = value;
        this.metadata = metadata;
        this.visibility = visibility;
    }

    protected void setValue(Object value) {
        this.value = value;
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

    @Override
    public int compareTo(Property o) {
        int i = getName().compareTo(o.getName());
        if (i != 0) {
            return i;
        }
        return getKey().compareTo(o.getKey());
    }

    @Override
    public int hashCode() {
        return getName().hashCode() ^ getKey().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Property) {
            Property other = (Property) obj;
            return getName().equals(other.getName()) && getKey().equals(other.getKey());
        }
        return super.equals(obj);
    }

    @Override
    public String toString() {
        return "[" + getName() + ":" + getKey() + "]";
    }
}
