package com.altamiracorp.securegraph;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.Set;

public class Text implements Serializable {
    private static final long serialVersionUID = 1L;
    private final String text;
    private final byte indexHint;

    public Text(String text) {
        this(text, TextIndexHint.ALL);
    }

    public Text(String text, TextIndexHint... indexHints) {
        this(text, EnumSet.copyOf(Arrays.asList(indexHints)));
    }

    public Text(String text, Collection<TextIndexHint> indexHints) {
        this.text = text;
        this.indexHint = TextIndexHint.toBits(indexHints);
    }

    public String getText() {
        return text;
    }

    public Set<TextIndexHint> getIndexHint() {
        return TextIndexHint.toSet(this.indexHint);
    }

    @Override
    public String toString() {
        return getText();
    }
}
