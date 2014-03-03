package com.altamiracorp.securegraph;

public class AlterPropertyVisibility {
    private final String key;
    private final String name;
    private final Visibility visibility;

    public AlterPropertyVisibility(String key, String name, Visibility visibility) {
        this.key = key;
        this.name = name;
        this.visibility = visibility;
    }

    public String getKey() {
        return key;
    }

    public String getName() {
        return name;
    }

    public Visibility getVisibility() {
        return visibility;
    }
}
