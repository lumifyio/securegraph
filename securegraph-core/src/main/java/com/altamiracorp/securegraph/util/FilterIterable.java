package com.altamiracorp.securegraph.util;

import java.util.Iterator;

public abstract class FilterIterable<T> implements Iterable<T> {
    private final Iterable<T> iterable;

    public FilterIterable(Iterable<T> iterable) {
        this.iterable = iterable;
    }

    @Override
    public Iterator<T> iterator() {
        final Iterator<T> it = iterable.iterator();
        return new Iterator<T>() {
            private T next;
            private T current;

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

                while (it.hasNext()) {
                    T obj = it.next();
                    if (isIncluded(obj)) {
                        continue;
                    }

                    this.next = obj;
                    break;
                }
            }
        };
    }

    protected abstract boolean isIncluded(T obj);
}
