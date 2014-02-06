package com.altamiracorp.securegraph.accumulo;

import com.altamiracorp.securegraph.SecureGraphException;
import com.altamiracorp.securegraph.property.StreamingPropertyValue;

import java.io.Serializable;

abstract class StreamingPropertyValueRef implements Serializable {
    private static final long serialVersionUID = 1L;
    private final String valueType;
    private final boolean searchIndex;
    private final boolean store;

    protected StreamingPropertyValueRef(StreamingPropertyValue propertyValue) {
        this.valueType = propertyValue.getValueType().getName();
        this.searchIndex = propertyValue.isSearchIndex();
        this.store = propertyValue.isStore();
    }

    public Class getValueType() {
        try {
            return Class.forName(valueType);
        } catch (ClassNotFoundException e) {
            throw new SecureGraphException("Could not get type: " + valueType);
        }
    }

    public boolean isSearchIndex() {
        return searchIndex;
    }

    public boolean isStore() {
        return store;
    }

    public abstract StreamingPropertyValue toStreamingPropertyValue(AccumuloGraph graph);
}
