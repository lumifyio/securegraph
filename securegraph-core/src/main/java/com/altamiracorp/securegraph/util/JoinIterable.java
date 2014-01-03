package com.altamiracorp.securegraph.util;

import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;

public class JoinIterable<T> implements Iterable<T> {
    private final Iterable<T>[] iterables;

    public JoinIterable(Iterable<T>... iterables) {
        this.iterables = iterables;
    }

    @Override
    public Iterator<T> iterator() {
        if (this.iterables.length == 0) {
            return new Iterator<T>() {
                @Override
                public boolean hasNext() {
                    return false;
                }

                @Override
                public T next() {
                    return null;
                }

                @Override
                public void remove() {

                }
            };
        }

        final Queue<Iterable<T>> iterables = new LinkedList<Iterable<T>>();
        Collections.addAll(iterables, this.iterables);
        final IteratorWrapper it = new IteratorWrapper();
        it.iterator = iterables.remove().iterator();

        return new Iterator<T>() {
            @Override
            public boolean hasNext() {
                while (true) {
                    if (it.iterator.hasNext()) {
                        return true;
                    }
                    if (iterables.size() == 0) {
                        return false;
                    }
                    it.iterator = iterables.remove().iterator();
                }
            }

            @Override
            public T next() {
                return it.iterator.next();
            }

            @Override
            public void remove() {
                it.iterator.remove();
            }
        };
    }

    private class IteratorWrapper {
        public Iterator<T> iterator;
    }
}
