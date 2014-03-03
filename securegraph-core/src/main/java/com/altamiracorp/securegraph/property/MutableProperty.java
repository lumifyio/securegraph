package com.altamiracorp.securegraph.property;

import com.altamiracorp.securegraph.Property;
import com.altamiracorp.securegraph.Visibility;

import java.util.Map;

public class MutableProperty extends Property {
    public MutableProperty(String key, String name, Object value, Map<String, Object> metadata, Visibility visibility) {
        super(key, name, value, metadata, visibility);
    }

    @Override
    public void setValue(Object value) {
        super.setValue(value);
    }

    @Override
    public void setVisibility(Visibility visibility) {
        super.setVisibility(visibility);
    }
}
