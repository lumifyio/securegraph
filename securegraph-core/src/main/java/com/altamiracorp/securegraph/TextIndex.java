package com.altamiracorp.securegraph;

import java.util.EnumSet;

public enum TextIndex {
    FULL_TEXT,
    EXACT_MATCH;

    public static EnumSet<TextIndex> BOTH = EnumSet.of(TextIndex.EXACT_MATCH, TextIndex.FULL_TEXT);
}
