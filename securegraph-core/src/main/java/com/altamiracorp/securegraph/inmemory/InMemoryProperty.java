package com.altamiracorp.securegraph.inmemory;

import com.altamiracorp.securegraph.Visibility;
import com.altamiracorp.securegraph.property.PropertyBase;

import java.util.Map;

public class InMemoryProperty extends PropertyBase {
    InMemoryProperty(Object id, String name, Object value, Map<String, Object> metadata, Visibility visibility) {
        super(id, name, value, metadata, visibility);
    }
}
