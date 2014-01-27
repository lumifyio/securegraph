package com.altamiracorp.securegraph.util;

import java.util.ArrayList;
import java.util.List;

public class IterableUtils {
    public static <T> List<T> toList(Iterable<T> iterable) {
        List<T> results = new ArrayList<T>();
        for (T o : iterable) {
            results.add(o);
        }
        return results;
    }

    public static <T> int count(Iterable<T> iterable) {
        int count = 0;
        for (T ignore : iterable) {
            count++;
        }
        return count;
    }
}
