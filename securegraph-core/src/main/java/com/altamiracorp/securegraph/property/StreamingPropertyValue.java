package com.altamiracorp.securegraph.property;

import com.altamiracorp.securegraph.Authorizations;

import java.io.InputStream;

public class StreamingPropertyValue extends PropertyValue {
    private final InputStream inputStream;
    private final Class valueType;

    public StreamingPropertyValue(InputStream inputStream, Class valueType) {
        this.inputStream = inputStream;
        this.valueType = valueType;
    }

    public InputStream getInputStream(Authorizations authorizations) {
        return inputStream;
    }

    public Class getValueType() {
        return valueType;
    }
}
