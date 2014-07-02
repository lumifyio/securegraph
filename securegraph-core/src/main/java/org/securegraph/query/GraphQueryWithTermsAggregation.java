package org.securegraph.query;

public interface GraphQueryWithTermsAggregation extends GraphQuery {
    GraphQueryWithTermsAggregation addTermsAggregation(String histogramName, String field);
}
