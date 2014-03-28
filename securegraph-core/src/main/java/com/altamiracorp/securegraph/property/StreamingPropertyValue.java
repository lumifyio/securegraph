package com.altamiracorp.securegraph.property;

import java.io.InputStream;

public class StreamingPropertyValue extends PropertyValue {
    private final InputStream inputStream;
    private final Class valueType;
    private final long length;

    public StreamingPropertyValue(InputStream inputStream, Class valueType) {
        this(inputStream, valueType, -1);
    }

    public StreamingPropertyValue(InputStream inputStream, Class valueType, long length) {
        this.inputStream = inputStream;
        this.valueType = valueType;
        this.length = length;
    }

    public InputStream getInputStream() {
        return inputStream;
    }

    public Class getValueType() {
        return valueType;
    }

    public long getLength() {
        return length;
    }
}
