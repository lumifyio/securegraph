package org.securegraph.mutation;

import org.securegraph.Property;
import org.securegraph.Visibility;

public class PropertyPropertyRemoveMutation extends PropertyRemoveMutation {
    private final Property property;

    public PropertyPropertyRemoveMutation(Property property) {
        this.property = property;
    }

    @Override
    public String getKey() {
        return property.getKey();
    }

    @Override
    public String getName() {
        return property.getName();
    }

    @Override
    public Visibility getVisibility() {
        return property.getVisibility();
    }

    public Property getProperty() {
        return property;
    }
}
