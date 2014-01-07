package com.altamiracorp.securegraph.accumulo;

import com.altamiracorp.securegraph.Visibility;
import com.altamiracorp.securegraph.property.PropertyBase;

import java.util.Map;

public class AccumuloProperty extends PropertyBase {
    protected AccumuloProperty(Object id, String name, Object value, Map<String, Object> metadata, Visibility visibility) {
        super(id, name, value, metadata, visibility);
    }
}
