package org.securegraph.elasticsearch;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilteredQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.securegraph.Authorizations;
import org.securegraph.Graph;
import org.securegraph.PropertyDefinition;

import java.util.List;
import java.util.Map;

public class ElasticSearchParentChildGraphQuery extends ElasticSearchGraphQueryBase {
    protected ElasticSearchParentChildGraphQuery(TransportClient client, String indexName, Graph graph, String queryString, Map<String, PropertyDefinition> propertyDefinitions, double inEdgeBoost, double outEdgeBoost, Authorizations authorizations) {
        super(client, indexName, graph, queryString, propertyDefinitions, inEdgeBoost, outEdgeBoost, false, authorizations);
    }

    @Override
    protected QueryBuilder createQuery(String queryString, List<FilterBuilder> filters) {
        // TODO it would be nice if we don't have to randomly remove the first element
        filters.remove(0); // the first filter is the element type.
        QueryBuilder query = super.createQuery(queryString, filters);
        FilterBuilder filterBuilder = getFilterBuilder(filters);
        FilteredQueryBuilder filteredQueryBuilder = QueryBuilders.filteredQuery(query, filterBuilder);
        return QueryBuilders.hasChildQuery(ElasticSearchParentChildSearchIndex.PROPERTY_TYPE, filteredQueryBuilder);
    }

    @Override
    protected SearchRequestBuilder getSearchRequestBuilder(List<FilterBuilder> filters, FunctionScoreQueryBuilder functionScoreQuery) {
        return getClient()
                .prepareSearch(getIndexName())
                .setTypes(ElasticSearchSearchIndexBase.ELEMENT_TYPE)
                .setQuery(functionScoreQuery)
                .setFrom((int) getParameters().getSkip())
                .setSize((int) getParameters().getLimit());
    }
}
