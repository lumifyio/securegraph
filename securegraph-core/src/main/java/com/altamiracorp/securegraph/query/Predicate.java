package com.altamiracorp.securegraph.query;

public interface Predicate {
    boolean evaluate(Iterable<Object> propertyValues, Object value);
}
