package com.altamiracorp.securegraph.query;

import com.altamiracorp.securegraph.Property;

public interface Predicate {
    boolean evaluate(Iterable<Property> properties, Object value);
}
