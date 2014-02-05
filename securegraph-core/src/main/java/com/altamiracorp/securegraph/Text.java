package com.altamiracorp.securegraph;

import java.io.Serializable;
import java.util.*;

public class Text implements Serializable {
    private final String text;
    private final Set<TextIndex> indexHint;

    public Text(String text) {
        this(text, TextIndex.ALL);
    }

    public Text(String text, TextIndex... indexHints) {
        this(text, EnumSet.copyOf(Arrays.asList(indexHints)));
    }

    public Text(String text, Collection<TextIndex> indexHints) {
        this.text = text;
        this.indexHint = EnumSet.copyOf(indexHints);
    }

    public String getText() {
        return text;
    }

    public Set<TextIndex> getIndexHint() {
        return Collections.unmodifiableSet(indexHint);
    }

    @Override
    public String toString() {
        return getText();
    }
}
