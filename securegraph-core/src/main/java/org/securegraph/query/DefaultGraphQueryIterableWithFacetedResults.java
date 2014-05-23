package org.securegraph.query;

import org.securegraph.Element;

import java.util.Map;

public class DefaultGraphQueryIterableWithFacetedResults<T extends Element> extends DefaultGraphQueryIterable<T> implements IterableWithFacetedResults<T> {
    private final Map<String, FacetedResult> facetedResults;
    private final long totalHits;

    public DefaultGraphQueryIterableWithFacetedResults(
            QueryBase.Parameters parameters, Iterable<T> iterable,
            boolean evaluateQueryString,
            boolean evaluateHasContainers,
            Map<String, FacetedResult> facetedResults,
            long totalHits) {
        super(parameters, iterable, evaluateQueryString, evaluateHasContainers);
        this.facetedResults = facetedResults;
        this.totalHits = totalHits;
    }

    @Override
    public FacetedResult getFacetedResult(String name) {
        return this.facetedResults.get(name);
    }

    @Override
    public long getTotalHits() {
        return totalHits;
    }
}
