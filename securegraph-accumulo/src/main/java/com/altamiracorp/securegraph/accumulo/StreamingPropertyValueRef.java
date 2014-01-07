package com.altamiracorp.securegraph.accumulo;

import com.altamiracorp.securegraph.SecureGraphException;
import com.altamiracorp.securegraph.property.StreamingPropertyValue;

import java.io.Serializable;

abstract class StreamingPropertyValueRef implements Serializable {
    private final String valueType;

    protected StreamingPropertyValueRef(Class valueType) {
        this.valueType = valueType.getName();
    }

    public Class getValueType() {
        try {
            return Class.forName(valueType);
        } catch (ClassNotFoundException e) {
            throw new SecureGraphException("Could not get type: " + valueType);
        }
    }

    public abstract StreamingPropertyValue toStreamingPropertyValue(AccumuloGraph graph);
}
