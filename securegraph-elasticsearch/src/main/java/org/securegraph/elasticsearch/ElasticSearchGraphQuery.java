package org.securegraph.elasticsearch;

import org.elasticsearch.client.transport.TransportClient;
import org.securegraph.Authorizations;
import org.securegraph.Graph;
import org.securegraph.PropertyDefinition;

import java.util.Map;

public class ElasticSearchGraphQuery extends ElasticSearchGraphQueryBase {
    public ElasticSearchGraphQuery(TransportClient client, String indexName, Graph graph, String queryString, Map<String, PropertyDefinition> propertyDefinitions, double inEdgeBoost, double outEdgeBoost, Authorizations authorizations) {
        super(client, indexName, graph, queryString, propertyDefinitions, inEdgeBoost, outEdgeBoost, true, authorizations);
    }
}
