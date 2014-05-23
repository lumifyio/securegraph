package org.securegraph.query;

public interface IterableWithFacetedResults<T> extends Iterable<T> {
    FacetedResult getFacetedResult(String facetName);

    long getTotalHits();
}
