package com.altamiracorp.securegraph;

import java.util.Map;

public interface Property {
    Object getId();

    String getName();

    Object getValue();

    Visibility getVisibility();

    Map<String, Object> getMetadata();
}
