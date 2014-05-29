package org.securegraph.query;

public interface IterableWithTotalHits<T> extends Iterable<T> {
    Long getTotalHits();
}
