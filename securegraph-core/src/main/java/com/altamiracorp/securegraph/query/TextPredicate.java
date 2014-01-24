package com.altamiracorp.securegraph.query;

import com.altamiracorp.securegraph.SecureGraphException;

public enum TextPredicate implements Predicate {
    CONTAINS;

    @Override
    public boolean evaluate(final Iterable<Object> propertyValues, final Object second) {
        for (Object propertyValue : propertyValues) {
            if (evaluate(propertyValue, second)) {
                return true;
            }
        }
        return false;
    }

    private boolean evaluate(Object first, Object second) {
        if (!(first instanceof String) || !(second instanceof String)) {
            throw new SecureGraphException("Text predicates are only valid for string fields");
        }

        String firstString = ((String) first).toLowerCase();
        String secondString = ((String) second).toLowerCase();

        switch (this) {
            case CONTAINS:
                return firstString.contains(secondString);
            default:
                throw new IllegalArgumentException("Invalid text predicate: " + this);
        }
    }
}
