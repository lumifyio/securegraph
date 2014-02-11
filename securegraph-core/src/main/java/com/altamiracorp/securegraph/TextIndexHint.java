package com.altamiracorp.securegraph;

import java.util.*;

public enum TextIndexHint {
    /**
     * Tokenize this property and index it for full text search.
     */
    FULL_TEXT((byte) 0x01),
    /**
     * Index this property for exact match searches.
     */
    EXACT_MATCH((byte) 0x02);

    private final byte value;

    TextIndexHint(byte value) {
        this.value = value;
    }

    public static byte toBits(TextIndexHint... indexHints) {
        return toBits(EnumSet.copyOf(Arrays.asList(indexHints)));
    }

    public static byte toBits(Collection<TextIndexHint> hints) {
        byte b = 0;
        for (TextIndexHint hint : hints) {
            b |= hint.value;
        }
        return b;
    }

    public static Set<TextIndexHint> toSet(byte indexHint) {
        Set<TextIndexHint> hints = new HashSet<TextIndexHint>();
        if ((indexHint & FULL_TEXT.value) == FULL_TEXT.value) {
            hints.add(FULL_TEXT);
        }
        if ((indexHint & EXACT_MATCH.value) == EXACT_MATCH.value) {
            hints.add(EXACT_MATCH);
        }
        return hints;
    }

    /**
     * Use this to prevent indexing of this Text property.  The property
     * will not be searchable.
     */
    public static final Set<TextIndexHint> NONE = EnumSet.noneOf(TextIndexHint.class);

    /**
     * The set of indexing hints that trigger all available indexes for
     * a Text property.
     */
    public static final Set<TextIndexHint> ALL = EnumSet.of(FULL_TEXT, EXACT_MATCH);
}
