package com.altamiracorp.securegraph.query;

public enum Compare implements Predicate {
    EQUAL, NOT_EQUAL, GREATER_THAN, GREATER_THAN_EQUAL, LESS_THAN, LESS_THAN_EQUAL, IN;

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
        switch (this) {
            case EQUAL:
                if (null == first) {
                    return second == null;
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
