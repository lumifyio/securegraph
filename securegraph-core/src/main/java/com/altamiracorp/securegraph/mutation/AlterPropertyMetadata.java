package com.altamiracorp.securegraph.mutation;

public class AlterPropertyMetadata {
    private final String propertyKey;
    private final String propertyName;
    private final String metadataName;
    private final Object newValue;

    public AlterPropertyMetadata(String propertyKey, String propertyName, String metadataName, Object newValue) {
        this.propertyKey = propertyKey;
        this.propertyName = propertyName;
        this.metadataName = metadataName;
        this.newValue = newValue;
    }

    public String getPropertyKey() {
        return propertyKey;
    }

    public String getPropertyName() {
        return propertyName;
    }

    public String getMetadataName() {
        return metadataName;
    }

    public Object getNewValue() {
        return newValue;
    }
}
