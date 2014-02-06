package com.altamiracorp.securegraph;

import java.io.Serializable;
import java.util.*;

public class Text implements Serializable {
    private static final long serialVersionUID = 1L;
    private final String text;
    private final Set<TextIndexHint> indexHint;

    public Text(String text) {
        this(text, TextIndexHint.ALL);
    }

    public Text(String text, TextIndexHint... indexHints) {
        this(text, EnumSet.copyOf(Arrays.asList(indexHints)));
    }

    public Text(String text, Collection<TextIndexHint> indexHints) {
        this.text = text;
        this.indexHint = EnumSet.copyOf(indexHints);
    }

    public String getText() {
        return text;
    }

    public Set<TextIndexHint> getIndexHint() {
        return Collections.unmodifiableSet(indexHint);
    }

    @Override
    public String toString() {
        return getText();
    }
}
