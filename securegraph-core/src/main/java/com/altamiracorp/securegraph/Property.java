package com.altamiracorp.securegraph;

public class Property {
    private final String name;
    private final Object value;
    private final Visibility visibility;

    public Property(String name, Object value, Visibility visibility) {
        this.name = name;
        this.value = value;
        this.visibility = visibility;
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
}
