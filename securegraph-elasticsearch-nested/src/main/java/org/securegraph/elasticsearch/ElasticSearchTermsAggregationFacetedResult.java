package org.securegraph.elasticsearch;

import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.securegraph.query.FacetedResult;
import org.securegraph.query.FacetedTerm;
import org.securegraph.util.ConvertingIterable;

public class ElasticSearchTermsAggregationFacetedResult extends FacetedResult {
    private final Terms terms;

    public ElasticSearchTermsAggregationFacetedResult(Terms terms) {
        this.terms = terms;
    }

    @Override
    public Iterable<FacetedTerm> getTerms() {
        return new ConvertingIterable<Terms.Bucket, FacetedTerm>(this.terms.getBuckets()) {
            @Override
            protected FacetedTerm convert(Terms.Bucket bucket) {
                return new FacetedTerm(bucket.getKey(), Long.valueOf(bucket.getDocCount()).intValue());
            }
        };
    }
}
