package org.securegraph.query;

public interface GraphQueryWithGeohashAggregation extends GraphQuery {
    GraphQueryWithGeohashAggregation addGeohashAggregation(String aggregationName, String field, int precision);
}
