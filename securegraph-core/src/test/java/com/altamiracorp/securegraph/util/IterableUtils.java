package com.altamiracorp.securegraph.util;

import java.util.Iterator;

public class IterableUtils {
    public static <T> int count(Iterable<T> iterable) {
        Iterator<T> it = iterable.iterator();
        int count = 0;
        while (it.hasNext()) {
            count++;
            it.next();
        }
        return count;
    }
}
