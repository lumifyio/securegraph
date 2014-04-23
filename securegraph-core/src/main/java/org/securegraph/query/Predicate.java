package org.securegraph.query;

import org.securegraph.Property;

public interface Predicate {
    boolean evaluate(Iterable<Property> properties, Object value);
}
