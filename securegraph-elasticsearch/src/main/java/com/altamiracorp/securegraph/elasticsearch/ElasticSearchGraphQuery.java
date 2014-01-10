package com.altamiracorp.securegraph.elasticsearch;

import com.altamiracorp.securegraph.*;
import com.altamiracorp.securegraph.query.Compare;
import com.altamiracorp.securegraph.query.GraphQueryBase;
import com.altamiracorp.securegraph.util.ConvertingIterable;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class ElasticSearchGraphQuery extends GraphQueryBase {
    private static final Logger LOGGER = LoggerFactory.getLogger(ElasticSearchGraphQuery.class);
    private final TransportClient client;
    private String indexName;

    public ElasticSearchGraphQuery(TransportClient client, String indexName, Graph graph, String queryString, Authorizations authorizations) {
        super(graph, queryString, authorizations);
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
        List<FilterBuilder> filters = new ArrayList<FilterBuilder>();
        filters.add(FilterBuilders.inFilter(ElasticSearchSearchIndex.ELEMENT_TYPE_FIELD_NAME, elementType));
        for (HasContainer has : getParameters().getHasContainers()) {
            if (has.predicate instanceof Compare) {
                Compare compare = (Compare) has.predicate;
                Object value = has.value;
                if (value instanceof String) {
                    value = ((String) value).toLowerCase(); // using the standard analyzer all strings are lower-cased.
                }
                switch (compare) {
                    case EQUAL:
                        filters.add(FilterBuilders.inFilter(has.key, value));
                        break;
                    case GREATER_THAN_EQUAL:
                        filters.add(FilterBuilders.rangeFilter(has.key).gte(value));
                        break;
                    case GREATER_THAN:
                        filters.add(FilterBuilders.rangeFilter(has.key).gt(value));
                        break;
                    case LESS_THAN_EQUAL:
                        filters.add(FilterBuilders.rangeFilter(has.key).lte(value));
                        break;
                    case LESS_THAN:
                        filters.add(FilterBuilders.rangeFilter(has.key).lt(value));
                        break;
                    case NOT_EQUAL:
                        filters.add(FilterBuilders.notFilter(FilterBuilders.inFilter(has.key, value)));
                        break;
                    default:
                        throw new SecureGraphException("Unexpected compare predicate " + has.predicate);
                }
            } else {
                throw new SecureGraphException("Unexpected predicate type " + has.predicate.getClass().getName());
            }
        }
        QueryBuilder query = createQuery(getParameters().getQueryString());
        SearchRequestBuilder q = client
                .prepareSearch(indexName)
                .setTypes(ElasticSearchSearchIndex.ELEMENT_TYPE)
                .setQuery(query)
                .setFilter(FilterBuilders.andFilter(filters.toArray(new FilterBuilder[filters.size()])))
                .setFrom((int) getParameters().getSkip())
                .setSize((int) getParameters().getLimit());
        LOGGER.debug("query: " + q);
        return q.execute()
                .actionGet();
    }

    protected QueryBuilder createQuery(String queryString) {
        QueryBuilder query;
        if (queryString == null) {
            query = QueryBuilders.matchAllQuery();
        } else {
            query = QueryBuilders.queryString(queryString);
        }
        return query;
    }
}
