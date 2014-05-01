package org.securegraph.query;

public abstract class FacetedResult {
    public abstract Iterable<FacetedTerm> getTerms();
}
