package org.securegraph.elasticsearch;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.query.FilterBuilder;
import org.securegraph.Authorizations;
import org.securegraph.Graph;
import org.securegraph.PropertyDefinition;

import java.util.List;
import java.util.Map;

public class ElasticSearchGraphQuery extends ElasticSearchGraphQueryBase {
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
}
