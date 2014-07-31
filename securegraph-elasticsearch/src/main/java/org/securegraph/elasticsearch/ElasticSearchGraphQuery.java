package org.securegraph.elasticsearch;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoHashGridBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.HistogramBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.securegraph.Authorizations;
import org.securegraph.Graph;
import org.securegraph.PropertyDefinition;
import org.securegraph.SecureGraphException;
import org.securegraph.query.*;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class ElasticSearchGraphQuery extends ElasticSearchGraphQueryBase implements
        GraphQueryWithHistogramAggregation,
        GraphQueryWithTermsAggregation,
        GraphQueryWithGeohashAggregation {
    private final List<HistogramQueryItem> histogramQueryItems = new ArrayList<HistogramQueryItem>();
    private final List<TermsQueryItem> termsQueryItems = new ArrayList<TermsQueryItem>();
    private final List<GeohashQueryItem> geohashQueryItems = new ArrayList<GeohashQueryItem>();

    public ElasticSearchGraphQuery(TransportClient client, String indexName, Graph graph, String queryString, Map<String, PropertyDefinition> propertyDefinitions, double inEdgeBoost, double outEdgeBoost, Authorizations authorizations) {
        super(client, indexName, graph, queryString, propertyDefinitions, inEdgeBoost, outEdgeBoost, false, authorizations);
    }

    @Override
    protected List<FilterBuilder> getFilters(String elementType) {
        List<FilterBuilder> filters = super.getFilters(elementType);

        AuthorizationFilterBuilder authorizationFilterBuilder = new AuthorizationFilterBuilder(getParameters().getAuthorizations().getAuthorizations());
        filters.add(authorizationFilterBuilder);

        return filters;
    }

    @Override
    public GraphQueryWithHistogramAggregation addHistogramAggregation(String aggregationName, String fieldName, String interval) {
        histogramQueryItems.add(new HistogramQueryItem(aggregationName, fieldName, interval));
        return this;
    }

    @Override
    public GraphQueryWithTermsAggregation addTermsAggregation(String aggregationName, String fieldName) {
        termsQueryItems.add(new TermsQueryItem(aggregationName, fieldName));
        return this;
    }

    @Override
    public GraphQueryWithGeohashAggregation addGeohashAggregation(String aggregationName, String fieldName, int precision) {
        geohashQueryItems.add(new GeohashQueryItem(aggregationName, fieldName, precision));
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
            Class propertyDataType = propertyDefinition.getDataType();
            if (propertyDataType == Date.class) {
                DateHistogramBuilder agg = AggregationBuilders.dateHistogram(histogramQueryItem.getAggregationName());
                agg.field(histogramQueryItem.getFieldName());
                agg.interval(Long.parseLong(histogramQueryItem.getInterval()));
                searchRequestBuilder.addAggregation(agg);
            } else {
                HistogramBuilder agg = AggregationBuilders.histogram(histogramQueryItem.getAggregationName());
                agg.field(histogramQueryItem.getFieldName());
                agg.interval(Long.parseLong(histogramQueryItem.getInterval()));
                searchRequestBuilder.addAggregation(agg);
            }
        }

        for (TermsQueryItem termsQueryItem : termsQueryItems) {
            TermsBuilder agg = AggregationBuilders.terms(termsQueryItem.getAggregationName());
            agg.field(termsQueryItem.getFieldName());
            searchRequestBuilder.addAggregation(agg);
        }

        for (GeohashQueryItem geohashQueryItem : geohashQueryItems) {
            GeoHashGridBuilder agg = AggregationBuilders.geohashGrid(geohashQueryItem.getAggregationName());
            agg.field(geohashQueryItem.getFieldName());
            agg.precision(geohashQueryItem.getPrecision());
            searchRequestBuilder.addAggregation(agg);
        }

        return searchRequestBuilder;
    }
}
