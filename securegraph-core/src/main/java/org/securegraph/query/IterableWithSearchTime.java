package org.securegraph.query;

public interface IterableWithSearchTime<T> extends Iterable<T> {
    long getSearchTimeNanoSeconds();
}
