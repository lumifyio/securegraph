package org.securegraph.query;

import java.util.Collection;

public abstract class FacetedResult {
    public abstract long getMissing();

    public abstract long getTotal();

    public abstract long getOther();

    public abstract Iterable<FacetedTerm> getTerms();
}
