package org.securegraph.query;

import org.securegraph.Property;
import org.securegraph.SecureGraphException;
import org.securegraph.Text;
import org.securegraph.TextIndexHint;

public enum TextPredicate implements Predicate {
    CONTAINS;

    @Override
    public boolean evaluate(final Iterable<Property> properties, final Object second) {
        for (Property property : properties) {
            if (evaluate(property, second)) {
                return true;
            }
        }
        return false;
    }

    private boolean evaluate(Property property, Object second) {
        Object first = property.getValue();
        if (!(first instanceof String || first instanceof Text) || !(second instanceof String || second instanceof Text)) {
            throw new SecureGraphException("Text predicates are only valid for string fields");
        }

        String firstString;
        if (first instanceof Text) {
            Text firstText = (Text) first;
            if (!firstText.getIndexHint().contains(TextIndexHint.FULL_TEXT)) {
                return false;
            }
            firstString = firstText.getText();
        } else {
            firstString = ((String) first);
        }
        firstString = firstString.toLowerCase();

        String secondString;
        if (second instanceof Text) {
            Text secondText = (Text) second;
            secondString = secondText.getText();
            if (!secondText.getIndexHint().contains(TextIndexHint.FULL_TEXT)) {
                return false;
            }
        } else {
            secondString = ((String) second);
        }
        secondString = secondString.toLowerCase();

        switch (this) {
            case CONTAINS:
                return firstString.contains(secondString);
            default:
                throw new IllegalArgumentException("Invalid text predicate: " + this);
        }
    }
}
