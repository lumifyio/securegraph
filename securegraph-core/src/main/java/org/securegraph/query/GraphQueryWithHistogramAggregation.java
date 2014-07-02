package org.securegraph.query;

public interface GraphQueryWithHistogramAggregation extends GraphQuery {
    GraphQueryWithHistogramAggregation addHistogramAggregation(String histogramName, String field, String interval);
}
