package com.altamiracorp.securegraph;

import java.util.EnumSet;
import java.util.Set;

public enum TextIndex {
    FULL_TEXT,
    EXACT_MATCH;

    public static final Set<TextIndex> ALL = EnumSet.allOf(TextIndex.class);
}
