package com.altamiracorp.securegraph;

import java.io.Serializable;
import java.util.EnumSet;

public class Text implements Serializable {
    private final String text;
    private final EnumSet<TextIndex> indexHint;

    public Text(String text) {
        this(text, TextIndex.BOTH);
    }

    public Text(String text, TextIndex indexHint) {
        this(text, EnumSet.of(indexHint));
    }

    public Text(String text, EnumSet<TextIndex> indexHint) {
        this.text = text;
        this.indexHint = indexHint;
    }

    public String getText() {
        return text;
    }

    public EnumSet<TextIndex> getIndexHint() {
        return indexHint;
    }

    @Override
    public String toString() {
        return String.format("%s (indexHint: %s)", getText(), getIndexHint());
    }
}
