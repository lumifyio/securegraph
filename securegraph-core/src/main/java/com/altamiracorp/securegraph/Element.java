package com.altamiracorp.securegraph;

public interface Element {
    Object getId();

    Property getProperty(String name);

    Visibility getVisibility();

    Object getPropertyValue(String name);
}
