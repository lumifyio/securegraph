package com.altamiracorp.securegraph.elasticsearch;

import com.altamiracorp.securegraph.*;
import com.altamiracorp.securegraph.query.Compare;
import com.altamiracorp.securegraph.query.GeoCompare;
import com.altamiracorp.securegraph.query.GraphQueryBase;
import com.altamiracorp.securegraph.query.TextPredicate;
import com.altamiracorp.securegraph.type.GeoCircle;
import com.altamiracorp.securegraph.util.ConvertingIterable;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static com.altamiracorp.securegraph.util.IterableUtils.toList;

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
        final SearchHits hits = response.getHits();
        List<Object> ids = toList(new ConvertingIterable<SearchHit, Object>(hits) {
            @Override
            protected Object convert(SearchHit searchHit) {
                return searchHit.getId();
            }
        });
        LOGGER.debug("elastic search results " + ids.size() + " of " + hits.getTotalHits());
        return getGraph().getVertices(ids, getParameters().getAuthorizations());
    }

    @Override
    public Iterable<Edge> edges() {
        SearchResponse response = getSearchResponse(ElasticSearchSearchIndex.ELEMENT_TYPE_EDGE);
        final SearchHits hits = response.getHits();
        List<Object> ids = toList(new ConvertingIterable<SearchHit, Object>(hits) {
            @Override
            protected Object convert(SearchHit searchHit) {
                return searchHit.getId();
            }
        });
        LOGGER.debug("elastic search results " + ids.size() + " of " + hits.getTotalHits());
        return getGraph().getEdges(ids, getParameters().getAuthorizations());
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
                        filters.add(FilterBuilders.termFilter(has.key, value));
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
                    case IN:
                        filters.add(FilterBuilders.inFilter(has.key, (Object[]) has.value));
                        break;
                    default:
                        throw new SecureGraphException("Unexpected Compare predicate " + has.predicate);
                }
            } else if (has.predicate instanceof TextPredicate) {
                TextPredicate compare = (TextPredicate) has.predicate;
                Object value = has.value;
                if (value instanceof String) {
                    value = ((String) value).toLowerCase(); // using the standard analyzer all strings are lower-cased.
                }
                switch (compare) {
                    case CONTAINS:
                        filters.add(FilterBuilders.termFilter(has.key, value));
                        break;
                    default:
                        throw new SecureGraphException("Unexpected text predicate " + has.predicate);
                }
            } else if (has.predicate instanceof GeoCompare) {
                GeoCompare compare = (GeoCompare) has.predicate;
                switch (compare) {
                    case WITHIN:
                        if (has.value instanceof GeoCircle) {
                            GeoCircle geoCircle = (GeoCircle) has.value;
                            double lat = geoCircle.getLatitude();
                            double lon = geoCircle.getLongitude();
                            double distance = geoCircle.getRadius();
                            filters.add(FilterBuilders.geoDistanceFilter(has.key).point(lat, lon).distance(distance, DistanceUnit.KILOMETERS));
                        } else {
                            throw new SecureGraphException("Unexpected has value type " + has.value.getClass().getName());
                        }
                        break;
                    default:
                        throw new SecureGraphException("Unexpected GeoCompare predicate " + has.predicate);
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
