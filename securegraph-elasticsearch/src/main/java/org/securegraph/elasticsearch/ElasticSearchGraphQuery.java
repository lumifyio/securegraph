package org.securegraph.elasticsearch;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.HistogramBuilder;
import org.securegraph.*;
import org.securegraph.query.GraphQueryWithHistogram;
import org.securegraph.query.HistogramQueryItem;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class ElasticSearchGraphQuery extends ElasticSearchGraphQueryBase implements GraphQueryWithHistogram {
    private final List<HistogramQueryItem> histogramQueryItems = new ArrayList<HistogramQueryItem>();

    public ElasticSearchGraphQuery(TransportClient client, String indexName, Graph graph, String queryString, Map<String, PropertyDefinition> propertyDefinitions, double inEdgeBoost, double outEdgeBoost, Authorizations authorizations) {
        super(client, indexName, graph, queryString, propertyDefinitions, inEdgeBoost, outEdgeBoost, true, authorizations);
    }

    @Override
    public GraphQueryWithHistogram addHistogram(String histogramName, String fieldName, String interval) {
        histogramQueryItems.add(new HistogramQueryItem(histogramName, fieldName, interval));
        return this;
    }

    @Override
    protected SearchRequestBuilder getSearchRequestBuilder(List<FilterBuilder> filters, FunctionScoreQueryBuilder functionScoreQuery) {
        SearchRequestBuilder searchRequestBuilder = super.getSearchRequestBuilder(filters, functionScoreQuery);

        for (HistogramQueryItem histogramQueryItem : histogramQueryItems) {
            PropertyDefinition propertyDefinition = getPropertyDefinitions().get(histogramQueryItem.getFieldName());
            if (propertyDefinition == null) {
                throw new SecureGraphException("Could not find mapping for property: " + histogramQueryItem.getFieldName());
            }
            if (propertyDefinition.getDataType() == Date.class) {
                DateHistogramBuilder agg = AggregationBuilders.dateHistogram(histogramQueryItem.getName());
                agg.field(histogramQueryItem.getFieldName());
                agg.interval(Long.parseLong(histogramQueryItem.getInterval()));
                searchRequestBuilder.addAggregation(agg);
            } else {
                HistogramBuilder agg = AggregationBuilders.histogram(histogramQueryItem.getName());
                agg.field(histogramQueryItem.getFieldName());
                agg.interval(Long.parseLong(histogramQueryItem.getInterval()));
                searchRequestBuilder.addAggregation(agg);
            }
        }

        return searchRequestBuilder;
    }
}
