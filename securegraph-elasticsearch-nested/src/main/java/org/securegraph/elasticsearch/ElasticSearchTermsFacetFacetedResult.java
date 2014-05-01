package org.securegraph.elasticsearch;

import org.elasticsearch.search.facet.terms.TermsFacet;
import org.securegraph.query.FacetedResult;
import org.securegraph.query.FacetedTerm;
import org.securegraph.util.ConvertingIterable;

public class ElasticSearchTermsFacetFacetedResult extends FacetedResult {
    private final TermsFacet termsFacet;

    public ElasticSearchTermsFacetFacetedResult(TermsFacet termsFacet) {
        this.termsFacet = termsFacet;
    }

    @Override
    public long getMissing() {
        return this.termsFacet.getMissingCount();
    }

    @Override
    public long getTotal() {
        return this.termsFacet.getTotalCount();
    }

    @Override
    public long getOther() {
        return this.termsFacet.getOtherCount();
    }

    @Override
    public Iterable<FacetedTerm> getTerms() {
        return new ConvertingIterable<TermsFacet.Entry, FacetedTerm>((Iterable<TermsFacet.Entry>) this.termsFacet.getEntries()) {
            @Override
            protected FacetedTerm convert(TermsFacet.Entry entry) {
                return new FacetedTerm(entry.getTerm().toString(), entry.getCount());
            }
        };
    }
}
