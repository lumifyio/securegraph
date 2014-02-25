package com.altamiracorp.securegraph.util;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class IterableUtils {
    public static <T> List<T> toList(Iterable<T> iterable) {
        if (iterable instanceof List) {
            return (List<T>) iterable;
        }
        List<T> results = new ArrayList<T>();
        for (T o : iterable) {
            results.add(o);
        }
        return results;
    }

    public static <T> Set<T> toSet(Iterable<T> iterable) {
        if (iterable instanceof Set) {
            return (Set<T>) iterable;
        }
        Set<T> results = new HashSet<T>();
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
