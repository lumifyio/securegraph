package com.altamiracorp.securegraph;

public abstract class ElementBase implements Element {
    public abstract Object getId();

    public abstract Property getProperty(String name);

    public abstract Visibility getVisibility();

    public Object getPropertyValue(String name) {
        Property prop = getProperty(name);
        if (prop == null) {
            return null;
        }
        return prop.getValue();
    }

    @Override
    public int hashCode() {
        return getId().hashCode();
    }

    @Override
    public String toString() {
        return getId().toString();
    }
}
