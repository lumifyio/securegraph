package org.securegraph.query;

import org.securegraph.Property;
import org.securegraph.PropertyDefinition;

import java.util.Map;

public interface Predicate {
    boolean evaluate(Iterable<Property> properties, Object value, Map<String, PropertyDefinition> propertyDefinitions);
}
