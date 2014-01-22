package com.altamiracorp.securegraph.property;

import java.io.InputStream;

public class StreamingPropertyValue extends PropertyValue {
    private final InputStream inputStream;
    private final Class valueType;

    public StreamingPropertyValue(InputStream inputStream, Class valueType) {
        this.inputStream = inputStream;
        this.valueType = valueType;
    }

    public InputStream getInputStream() {
        return inputStream;
    }

    public Class getValueType() {
        return valueType;
    }
}
