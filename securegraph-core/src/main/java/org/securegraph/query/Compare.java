package org.securegraph.query;

import org.securegraph.DateOnly;
import org.securegraph.Property;
import org.securegraph.Text;
import org.securegraph.TextIndexHint;

import java.util.Date;

public enum Compare implements Predicate {
    EQUAL, NOT_EQUAL, GREATER_THAN, GREATER_THAN_EQUAL, LESS_THAN, LESS_THAN_EQUAL, IN;

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

        if (first instanceof DateOnly) {
            first = ((DateOnly) first).getDate();
            if (second instanceof Date) {
                second = new DateOnly((Date) second).getDate();
            }
        }
        if (second instanceof DateOnly) {
            second = ((DateOnly) second).getDate();
            if (first instanceof Date) {
                first = new DateOnly((Date) first).getDate();
            }
        }

        switch (this) {
            case EQUAL:
                if (null == first) {
                    return second == null;
                }
                if (first instanceof Text) {
                    Text firstText = (Text) first;
                    if (!firstText.getIndexHint().contains(TextIndexHint.EXACT_MATCH)) {
                        return false;
                    }
                    first = firstText.getText();
                }
                if (second instanceof Text) {
                    Text secondText = (Text) second;
                    if (!secondText.getIndexHint().contains(TextIndexHint.EXACT_MATCH)) {
                        return false;
                    }
                    second = secondText.getText();
                }
                return first.equals(second);
            case NOT_EQUAL:
                if (null == first) {
                    return second != null;
                }
                return !first.equals(second);
            case GREATER_THAN:
                if (null == first || second == null) {
                    return false;
                }
                return ((Comparable) first).compareTo(second) >= 1;
            case LESS_THAN:
                if (null == first || second == null) {
                    return false;
                }
                return ((Comparable) first).compareTo(second) <= -1;
            case GREATER_THAN_EQUAL:
                if (null == first || second == null) {
                    return false;
                }
                return ((Comparable) first).compareTo(second) >= 0;
            case LESS_THAN_EQUAL:
                if (null == first || second == null) {
                    return false;
                }
                return ((Comparable) first).compareTo(second) <= 0;
            case IN:
                if (first instanceof Text) {
                    first = first.toString();
                }
                return evaluateIn(first, (Object[]) second);
            default:
                throw new IllegalArgumentException("Invalid compare: " + this);
        }
    }

    private boolean evaluateIn(Object first, Object[] second) {
        for (Object o : second) {
            if (first.equals(o)) {
                return true;
            }
        }
        return false;
    }
}
