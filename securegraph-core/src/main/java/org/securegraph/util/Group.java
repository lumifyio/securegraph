package org.securegraph.util;

import java.util.List;

public class Group<T> {
    private final String key;
    private final List<T> items;

    public Group(String key, List<T> items) {
        this.key = key;
        this.items = items;
    }

    public String getKey() {
        return key;
    }

    public List<T> getItems() {
        return items;
    }
}
