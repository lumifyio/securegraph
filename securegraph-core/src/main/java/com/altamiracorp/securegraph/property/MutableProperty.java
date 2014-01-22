package com.altamiracorp.securegraph.property;

import com.altamiracorp.securegraph.Property;
import com.altamiracorp.securegraph.Visibility;

import java.util.Map;

public class MutableProperty extends Property {
    public MutableProperty(Object id, String name, Object value, Map<String, Object> metadata, Visibility visibility) {
        super(id, name, value, metadata, visibility);
    }

    @Override
    public void setValue(Object value) {
        super.setValue(value);
    }
}
