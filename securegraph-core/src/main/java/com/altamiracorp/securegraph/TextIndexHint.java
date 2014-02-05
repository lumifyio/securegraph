package com.altamiracorp.securegraph;

import java.util.EnumSet;
import java.util.Set;

public enum TextIndexHint {
    /**
     * Tokenize this property and index it for full text search.
     */
    FULL_TEXT,
    /**
     * Index this property for exact match searches.
     */
    EXACT_MATCH;

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
