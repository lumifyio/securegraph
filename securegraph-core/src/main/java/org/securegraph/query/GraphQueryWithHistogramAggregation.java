package org.securegraph.query;

public interface GraphQueryWithHistogramAggregation extends GraphQuery {
    GraphQueryWithHistogramAggregation addHistogramAggregation(String aggregationName, String field, String interval);
}
