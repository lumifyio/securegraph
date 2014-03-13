package com.altamiracorp.securegraph.query;

import com.altamiracorp.securegraph.Element;
import com.altamiracorp.securegraph.Property;

import java.util.Iterator;

public class DefaultGraphQueryIterable<T extends Element> implements Iterable<T> {
    private final QueryBase.Parameters parameters;
    private final Iterable<T> iterable;

    public DefaultGraphQueryIterable(QueryBase.Parameters parameters, Iterable<T> iterable) {
        this.parameters = parameters;
        this.iterable = iterable;
    }

    @Override
    public Iterator<T> iterator() {
        final Iterator<T> it = iterable.iterator();

        return new Iterator<T>() {
            public T next;
            public T current;
            public long count;

            @Override
            public boolean hasNext() {
                loadNext();
                return next != null;
            }

            @Override
            public T next() {
                loadNext();
                this.current = this.next;
                this.next = null;
                return this.current;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }

            private void loadNext() {
                if (this.next != null) {
                    return;
                }

                if (this.count >= parameters.getLimit()) {
                    return;
                }

                while (it.hasNext()) {
                    T elem = it.next();

                    boolean match = true;
                    for (QueryBase.HasContainer has : parameters.getHasContainers()) {
                        if (!has.isMatch(elem)) {
                            match = false;
                            break;
                        }
                    }
                    if (!match) {
                        continue;
                    }
                    if (parameters.getQueryString() != null && !evaluateQueryString(elem, parameters.getQueryString())) {
                        continue;
                    }

                    this.count++;
                    if (this.count <= parameters.getSkip()) {
                        continue;
                    }

                    this.next = elem;
                    break;
                }
            }
        };
    }

    protected boolean evaluateQueryString(Element elem, String queryString) {
        for (Property property : elem.getProperties()) {
            if (evaluateQueryStringOnValue(property.getValue(), queryString)) {
                return true;
            }
        }
        return false;
    }

    private boolean evaluateQueryStringOnValue(Object value, String queryString) {
        if (value == null) {
            return false;
        }
        String valueString = value.toString().toLowerCase();
        return valueString.contains(queryString.toLowerCase());
    }
}
