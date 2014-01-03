package com.altamiracorp.securegraph.elasticsearch;

import com.altamiracorp.securegraph.Authorizations;
import com.altamiracorp.securegraph.Edge;
import com.altamiracorp.securegraph.Graph;
import com.altamiracorp.securegraph.Vertex;
import com.altamiracorp.securegraph.query.GraphQueryBase;
import com.altamiracorp.securegraph.util.ConvertingIterable;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

public class ElasticSearchGraphQuery extends GraphQueryBase {
    private final TransportClient client;
    private String indexName;

    public ElasticSearchGraphQuery(TransportClient client, String indexName, Graph graph, Authorizations authorizations) {
        super(graph, authorizations);
        this.client = client;
        this.indexName = indexName;
    }

    @Override
    public Iterable<Vertex> vertices() {
        SearchResponse response = getSearchResponse(ElasticSearchSearchIndex.ELEMENT_TYPE_VERTEX);
        return new ConvertingIterable<SearchHit, Vertex>(response.getHits()) {
            @Override
            protected Vertex convert(SearchHit searchHit) {
                String id = searchHit.getId();
                return getGraph().getVertex(id, getParameters().getAuthorizations());
            }
        };
    }

    @Override
    public Iterable<Edge> edges() {
        SearchResponse response = getSearchResponse(ElasticSearchSearchIndex.ELEMENT_TYPE_EDGE);
        return new ConvertingIterable<SearchHit, Edge>(response.getHits()) {
            @Override
            protected Edge convert(SearchHit searchHit) {
                String id = searchHit.getId();
                return getGraph().getEdge(id, getParameters().getAuthorizations());
            }
        };
    }

    private SearchResponse getSearchResponse(String elementType) {
        return client
                .prepareSearch(indexName)
                .setTypes(ElasticSearchSearchIndex.ELEMENT_TYPE)
                .setQuery(QueryBuilders.matchAllQuery())
                .setFilter(FilterBuilders.inFilter(ElasticSearchSearchIndex.ELEMENT_TYPE_FIELD_NAME, elementType))
                .execute()
                .actionGet();
    }
}
