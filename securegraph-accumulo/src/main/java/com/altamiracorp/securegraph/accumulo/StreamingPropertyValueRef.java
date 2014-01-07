package com.altamiracorp.securegraph.accumulo;

import com.altamiracorp.securegraph.SecureGraphException;

import java.io.Serializable;

class StreamingPropertyValueRef implements Serializable {
    private final String valueType;

    public StreamingPropertyValueRef(Class valueType) {
        this.valueType = valueType.getName();
    }

    public Class getValueType() {
        try {
            return Class.forName(valueType);
        } catch (ClassNotFoundException e) {
            throw new SecureGraphException("Could not get type: " + valueType);
        }
    }
}
