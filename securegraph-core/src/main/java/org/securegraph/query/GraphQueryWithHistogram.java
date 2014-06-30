package org.securegraph.query;

public interface GraphQueryWithHistogram extends GraphQuery {
    GraphQueryWithHistogram addHistogram(String histogramName, String field, String interval);
}
