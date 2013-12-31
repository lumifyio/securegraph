package com.altamiracorp.securegraph;

import com.altamiracorp.securegraph.util.WrappingIterable;

public abstract class ElementBase implements Element {
    @Override
    public Iterable<Object> getPropertyValues(final String name) {
        return new WrappingIterable<Property, Object>(getProperties(name)) {
            @Override
            protected Object convert(Property p) {
                return p.getValue();
            }
        };
    }

    @Override
    public int hashCode() {
        return getId().hashCode();
    }

    @Override
    public String toString() {
        return getId().toString();
    }
}
