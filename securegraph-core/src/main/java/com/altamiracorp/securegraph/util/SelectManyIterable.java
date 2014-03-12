package com.altamiracorp.securegraph.util;

import java.util.Iterator;

public abstract class SelectManyIterable<TSource, TDest> implements Iterable<TDest> {
    private final Iterable<TSource> source;

    public SelectManyIterable(Iterable<TSource> source) {
        this.source = source;
    }

    @Override
    public Iterator<TDest> iterator() {
        final Iterator<TSource> it = source.iterator();
        return new Iterator<TDest>() {
            private TDest next;
            private TDest current;
            public Iterator<TDest> innerIterator = null;

            @Override
            public boolean hasNext() {
                loadNext();
                return next != null;
            }

            @Override
            public TDest next() {
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
                while (true) {
                    if (innerIterator != null) {
                        if (innerIterator.hasNext()) {
                            this.next = innerIterator.next();
                            return;
                        } else {
                            innerIterator = null;
                        }
                    } else {
                        if (it.hasNext()) {
                            TSource nextSource = it.next();
                            innerIterator = getIterable(nextSource).iterator();
                        } else {
                            return;
                        }
                    }
                }
            }
        };
    }

    public abstract Iterable<TDest> getIterable(TSource source);
}
