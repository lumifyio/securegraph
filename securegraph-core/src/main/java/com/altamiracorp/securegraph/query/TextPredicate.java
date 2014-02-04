package com.altamiracorp.securegraph.query;

import com.altamiracorp.securegraph.SecureGraphException;
import com.altamiracorp.securegraph.Text;
import com.altamiracorp.securegraph.TextIndex;

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
        if (!(first instanceof String || first instanceof Text) || !(second instanceof String || second instanceof Text)) {
            throw new SecureGraphException("Text predicates are only valid for string fields");
        }

        String firstString;
        if (first instanceof Text) {
            Text firstText = (Text) first;
            if (!firstText.getIndexHint().contains(TextIndex.FULL_TEXT)) {
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
            if (!secondText.getIndexHint().contains(TextIndex.FULL_TEXT)) {
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
