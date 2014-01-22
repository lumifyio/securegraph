package com.altamiracorp.securegraph.query;

import com.altamiracorp.securegraph.type.GeoShape;

public enum GeoCompare implements Predicate {
    WITHIN;

    @Override
    public boolean evaluate(Iterable<Object> propertyValues, Object second) {
        for (Object propertyValue : propertyValues) {
            if (evaluate(propertyValue, second)) {
                return true;
            }
        }
        return false;
    }

    private boolean evaluate(Object first, Object second) {
        switch (this) {
            case WITHIN:
                return ((GeoShape) second).within((GeoShape) first);
            default:
                throw new IllegalArgumentException("Invalid compare: " + this);
        }
    }
}