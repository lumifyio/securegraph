package org.securegraph.query;

import org.securegraph.Property;
import org.securegraph.type.GeoShape;

public enum GeoCompare implements Predicate {
    WITHIN;

    @Override
    public boolean evaluate(Iterable<Property> properties, Object second) {
        for (Property property : properties) {
            if (evaluate(property, second)) {
                return true;
            }
        }
        return false;
    }

    private boolean evaluate(Property property, Object second) {
        switch (this) {
            case WITHIN:
                return ((GeoShape) second).within((GeoShape) property.getValue());
            default:
                throw new IllegalArgumentException("Invalid compare: " + this);
        }
    }
}