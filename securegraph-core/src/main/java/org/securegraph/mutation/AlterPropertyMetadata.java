package org.securegraph.mutation;

import org.securegraph.Visibility;

public class AlterPropertyMetadata {
    private final String propertyKey;
    private final String propertyName;
    private final Visibility propertyVisibility;
    private final String metadataName;
    private final Object newValue;

    public AlterPropertyMetadata(String propertyKey, String propertyName, Visibility propertyVisibility, String metadataName, Object newValue) {
        this.propertyKey = propertyKey;
        this.propertyName = propertyName;
        this.propertyVisibility = propertyVisibility;
        this.metadataName = metadataName;
        this.newValue = newValue;
    }

    public String getPropertyKey() {
        return propertyKey;
    }

    public String getPropertyName() {
        return propertyName;
    }

    public Visibility getPropertyVisibility() {
        return propertyVisibility;
    }

    public String getMetadataName() {
        return metadataName;
    }

    public Object getNewValue() {
        return newValue;
    }
}
