package com.altamiracorp.securegraph;

public class Visibility implements Comparable<Visibility> {
    private final String visibilityString;

    public Visibility(String visibilityString) {
        this.visibilityString = visibilityString;
    }

    public String getVisibilityString() {
        return visibilityString;
    }

    @Override
    public String toString() {
        return getVisibilityString();
    }

    @Override
    public int hashCode() {
        return getVisibilityString().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Visibility) {
            Visibility objVisibility = (Visibility) obj;
            return getVisibilityString().equals(objVisibility.getVisibilityString());
        }
        return super.equals(obj);
    }

    @Override
    public int compareTo(Visibility o) {
        return getVisibilityString().compareTo(o.getVisibilityString());
    }
}
