package org.securegraph.query;

import org.securegraph.Property;
import org.securegraph.PropertyDefinition;
import org.securegraph.SecureGraphException;
import org.securegraph.TextIndexHint;
import org.securegraph.type.GeoPoint;

import java.util.Map;

public enum TextPredicate implements Predicate {
    CONTAINS;

    @Override
    public boolean evaluate(final Iterable<Property> properties, final Object second, Map<String, PropertyDefinition> propertyDefinitions) {
        for (Property property : properties) {
            PropertyDefinition propertyDefinition = propertyDefinitions.get(property.getName());
            if (evaluate(property, second, propertyDefinition)) {
                return true;
            }
        }
        return false;
    }

    private boolean evaluate(Property property, Object second, PropertyDefinition propertyDefinition) {
        Object first = property.getValue();
        if (!(first instanceof String || first instanceof GeoPoint) || !(second instanceof String || second instanceof GeoPoint)) {
            throw new SecureGraphException("Text predicates are only valid for string or GeoPoint fields");
        }

        if (first instanceof GeoPoint) {
            first = ((GeoPoint) first).getDescription();
        }

        if (second instanceof GeoPoint) {
            second = ((GeoPoint) second).getDescription();
        }

        String firstString = (String) first;
        firstString = firstString.toLowerCase();

        String secondString =   (String) second;
        secondString = secondString.toLowerCase();

        switch (this) {
            case CONTAINS:
                if (propertyDefinition != null && !propertyDefinition.getTextIndexHints().contains(TextIndexHint.FULL_TEXT)) {
                    return false;
                }
                return firstString.contains(secondString);
            default:
                throw new IllegalArgumentException("Invalid text predicate: " + this);
        }
    }
}
