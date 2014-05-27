package org.securegraph.elasticsearch;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.query.*;
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
    protected QueryBuilder createQuery(String queryString, String elementType, List<FilterBuilder> filters) {
        FilterBuilder elementTypeFilter = createElementTypeFilter(elementType);
        AndFilterBuilder andFilterBuilder = FilterBuilders.andFilter(
                elementTypeFilter,
                new AuthorizationFilterBuilder(getParameters().getAuthorizations().getAuthorizations())
        );

        QueryBuilder hasChildQuery;
        if ((queryString != null && queryString.length() > 0) || (filters.size() > 1)) { // if the only filter is the authorizations filter skip creating the child filter
            QueryBuilder query = super.createQuery(queryString, elementType, filters);
            FilterBuilder filterBuilder = getFilterBuilder(filters);
            final FilteredQueryBuilder filteredQueryBuilder = QueryBuilders.filteredQuery(query, filterBuilder);
            hasChildQuery = new HasChildQueryBuilder(ElasticSearchParentChildSearchIndex.PROPERTY_TYPE, filteredQueryBuilder).scoreType("avg");
        } else {
            hasChildQuery = QueryBuilders.matchAllQuery();
        }

        return QueryBuilders.filteredQuery(
                hasChildQuery,
                andFilterBuilder
        );
    }

    @Override
    protected List<FilterBuilder> getFilters(String elementType) {
        List<FilterBuilder> filters = super.getFilters(elementType);
        filters.add(new AuthorizationFilterBuilder(getParameters().getAuthorizations().getAuthorizations()));
        return filters;
    }

    @Override
    protected void addElementTypeFilter(List<FilterBuilder> filters, String elementType) {
        // don't add the element type filter here because the child docs don't have element type only the parent type does
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

    @Override
    protected void addNotFilter(List<FilterBuilder> filters, String key, Object value) {
        filters.add(FilterBuilders.existsFilter(key));
        super.addNotFilter(filters, key, value);
    }
}
