package org.securegraph.query;

import org.securegraph.Element;

import java.util.Map;

public class DefaultGraphQueryIterableWithFacetedResults<T extends Element> extends DefaultGraphQueryIterable<T> implements IterableWithFacetedResults<T> {
    private final Map<String, FacetedResult> facetedResults;

    public DefaultGraphQueryIterableWithFacetedResults(QueryBase.Parameters parameters, Iterable<T> iterable, boolean evaluateQueryString, Map<String, FacetedResult> facetedResults) {
        super(parameters, iterable, evaluateQueryString);
        this.facetedResults = facetedResults;
    }

    @Override
    public FacetedResult getFacetedResult(String name) {
        return this.facetedResults.get(name);
    }
}
