package com.altamiracorp.securegraph.util;

import java.util.Iterator;

public class EmptyClosableIterable<T> implements ClosableIterable<T> {
    @Override
    public void close() {

    }

    @Override
    public Iterator<T> iterator() {
        return new Iterator<T>() {
            @Override
            public boolean hasNext() {
                return false;
            }

            @Override
            public T next() {
                throw new IllegalStateException("No element");
            }

            @Override
            public void remove() {
                throw new IllegalStateException("Remove not implemented");
            }
        };
    }
}
