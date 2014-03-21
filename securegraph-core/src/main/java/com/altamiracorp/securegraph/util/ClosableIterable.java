package com.altamiracorp.securegraph.util;

public interface ClosableIterable<T> extends Iterable<T> {
    void close();
}
