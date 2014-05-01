package org.securegraph.elasticsearch;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.geo.builders.CircleBuilder;
import org.elasticsearch.common.geo.builders.ShapeBuilder;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.facet.FacetBuilders;
import org.elasticsearch.search.facet.Facets;
import org.elasticsearch.search.facet.terms.TermsFacet;
import org.elasticsearch.search.facet.terms.TermsFacetBuilder;
import org.securegraph.*;
import org.securegraph.query.*;
import org.securegraph.type.GeoCircle;
import org.securegraph.util.ConvertingIterable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.securegraph.util.IterableUtils.toList;

public class ElasticSearchNestedGraphQuery extends GraphQueryBase implements QuerySupportingFacetedResults {
    private static final Logger LOGGER = LoggerFactory.getLogger(ElasticSearchNestedGraphQuery.class);
    private final TransportClient client;
    private String indexName;
    private List<Facet> facets = new ArrayList<Facet>();

    public ElasticSearchNestedGraphQuery(TransportClient client, String indexName, Graph graph, String queryString, Authorizations authorizations) {
        super(graph, queryString, authorizations);
        this.client = client;
        this.indexName = indexName;
    }

    @Override
    public Iterable<Vertex> vertices() {
        long startTime = System.nanoTime();
        SearchResponse response = getSearchResponse(ElasticSearchNestedSearchIndex.ELEMENT_TYPE_VERTEX);
        Map<String, FacetedResult> facetedResult = toFacetedResults(response.getFacets());
        final SearchHits hits = response.getHits();
        List<Object> ids = toList(new ConvertingIterable<SearchHit, Object>(hits) {
            @Override
            protected Object convert(SearchHit searchHit) {
                return searchHit.getId();
            }
        });
        long endTime = System.nanoTime();
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("elastic search results " + ids.size() + " of " + hits.getTotalHits() + " (time: " + ((endTime - startTime) / 1000 / 1000) + "ms)");
        }

        // since ES doesn't support security we will rely on the graph to provide vertex filtering
        // and rely on the DefaultGraphQueryIterable to provide property filtering
        Parameters filterParameters = getParameters().clone();
        filterParameters.setSkip(0); // ES already did a skip
        Iterable<Vertex> vertices = getGraph().getVertices(ids, filterParameters.getAuthorizations());
        return new DefaultGraphQueryIterableWithFacetedResults<Vertex>(filterParameters, vertices, false, facetedResult);
    }

    @Override
    public Iterable<Edge> edges() {
        long startTime = System.nanoTime();
        SearchResponse response = getSearchResponse(ElasticSearchNestedSearchIndex.ELEMENT_TYPE_EDGE);
        Map<String, FacetedResult> facetedResult = toFacetedResults(response.getFacets());
        final SearchHits hits = response.getHits();
        List<Object> ids = toList(new ConvertingIterable<SearchHit, Object>(hits) {
            @Override
            protected Object convert(SearchHit searchHit) {
                return searchHit.getId();
            }
        });
        long endTime = System.nanoTime();
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("elastic search results " + ids.size() + " of " + hits.getTotalHits() + " (time: " + ((endTime - startTime) / 1000 / 1000) + "ms)");
        }

        // since ES doesn't support security we will rely on the graph to provide edge filtering
        // and rely on the DefaultGraphQueryIterable to provide property filtering
        Parameters filterParameters = getParameters().clone();
        filterParameters.setSkip(0); // ES already did a skip
        Iterable<Edge> edges = getGraph().getEdges(ids, filterParameters.getAuthorizations());
        // TODO instead of passing false here to not evaluate the query string it would be better to support the Lucene query
        return new DefaultGraphQueryIterableWithFacetedResults<Edge>(filterParameters, edges, false, facetedResult);
    }

    private Map<String, FacetedResult> toFacetedResults(Facets facets) {
        Map<String, FacetedResult> facetedResults = new HashMap<String, FacetedResult>();
        if (facets == null || facets.facets() == null) {
            return facetedResults;
        }
        for (org.elasticsearch.search.facet.Facet esFacet : facets.facets()) {
            facetedResults.put(esFacet.getName(), toFacetedResult(esFacet));
        }
        return facetedResults;
    }

    private FacetedResult toFacetedResult(org.elasticsearch.search.facet.Facet esFacet) {
        if (esFacet instanceof TermsFacet) {
            TermsFacet termsFacet = (TermsFacet) esFacet;
            return new ElasticSearchTermsFacetFacetedResult(termsFacet);
        } else {
            throw new SecureGraphException("Invalid facet type: " + esFacet.getClass().getName());
        }
    }

    private SearchResponse getSearchResponse(String elementType) {
        List<QueryBuilder> nestedQueries = new ArrayList<QueryBuilder>();

        for (HasContainer has : getParameters().getHasContainers()) {
            if (has.predicate instanceof Compare) {
                Compare compare = (Compare) has.predicate;
                Object value = has.value;
                String key = has.key;
                if (value instanceof String || value instanceof String[]) {
                    key = key + ElasticSearchNestedSearchIndex.EXACT_MATCH_PROPERTY_NAME_SUFFIX;
                }
                switch (compare) {
                    case EQUAL:
                        if (value instanceof DateOnly) {
                            DateOnly dateOnlyValue = ((DateOnly) value);
                            RangeQueryBuilder dateOnlyQuery = QueryBuilders.rangeQuery(key).from(dateOnlyValue.toString()).to(dateOnlyValue.toString());
                            nestedQueries.add(nestedPropertyQuery(dateOnlyQuery));
                        } else {
                            TermQueryBuilder termQuery = QueryBuilders.termQuery(key, value);
                            nestedQueries.add(nestedPropertyQuery(termQuery));
                        }
                        break;
                    case GREATER_THAN_EQUAL:
                        RangeQueryBuilder gteQuery = QueryBuilders.rangeQuery(key).gte(value);
                        nestedQueries.add(nestedPropertyQuery(gteQuery));
                        break;
                    case GREATER_THAN:
                        RangeQueryBuilder gtQuery = QueryBuilders.rangeQuery(key).gt(value);
                        nestedQueries.add(nestedPropertyQuery(gtQuery));
                        break;
                    case LESS_THAN_EQUAL:
                        RangeQueryBuilder lteQuery = QueryBuilders.rangeQuery(key).lte(value);
                        nestedQueries.add(nestedPropertyQuery(lteQuery));
                        break;
                    case LESS_THAN:
                        RangeQueryBuilder ltQuery = QueryBuilders.rangeQuery(key).lt(value);
                        nestedQueries.add(nestedPropertyQuery(ltQuery));
                        break;
                    case NOT_EQUAL:
                        TermQueryBuilder termQuery = QueryBuilders.termQuery(key, value);
                        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
                        boolQuery.mustNot(termQuery);
                        nestedQueries.add(nestedPropertyQuery(boolQuery));
                        break;
                    case IN:
                        TermsQueryBuilder inQuery = QueryBuilders.inQuery(key, (Object[]) has.value);
                        nestedQueries.add(nestedPropertyQuery(inQuery));
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
                        if (value instanceof String) {
                            TermsQueryBuilder termsQuery = QueryBuilders.termsQuery(has.key, splitStringIntoTerms((String) value));
                            nestedQueries.add(nestedPropertyQuery(termsQuery));
                        } else {
                            TermQueryBuilder termQuery = QueryBuilders.termQuery(has.key, value);
                            nestedQueries.add(nestedPropertyQuery(termQuery));
                        }
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
                            CircleBuilder circleBuilder = ShapeBuilder.newCircleBuilder().center(lon, lat).radius(distance, DistanceUnit.KILOMETERS);
                            GeoShapeQueryBuilder geoQuery = QueryBuilders.geoShapeQuery(has.key, circleBuilder);
                            nestedQueries.add(nestedPropertyQuery(geoQuery));
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

        BoolQueryBuilder query = QueryBuilders.boolQuery();
        TermQueryBuilder elementTypeQuery = QueryBuilders.termQuery(ElasticSearchNestedSearchIndex.ELEMENT_TYPE_FIELD_NAME, elementType);
        query.must(elementTypeQuery);

        String queryString = getParameters().getQueryString();
        if (queryString != null && !queryString.equals("")) {
            QueryStringQueryBuilder queryStringQuery = QueryBuilders.queryString(getParameters().getQueryString());
            query.must(queryStringQuery);
        }

        for (QueryBuilder builder : nestedQueries) {
            query.must(builder);
        }

        SearchRequestBuilder q = client
                .prepareSearch(indexName)
                .setTypes(ElasticSearchNestedSearchIndex.ELEMENT_TYPE)
                .setQuery(query)
                .setFrom((int) getParameters().getSkip())
                .setSize((int) getParameters().getLimit());

        for (Facet facet : this.facets) {
            if (facet instanceof TermFacet) {
                TermFacet termFacet = (TermFacet) facet;
                TermsFacetBuilder esFacets = FacetBuilders.termsFacet(termFacet.getName())
                        .field(termFacet.getPropertyName())
                        .size(1000);
                q.addFacet(esFacets);
            } else {
                throw new SecureGraphException("Unsupported facet type: " + facet.getClass().getName());
            }
        }

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("query: " + q);
        }

        return q.execute()
                .actionGet();
    }

    private NestedQueryBuilder nestedPropertyQuery(QueryBuilder propertyQuery) {
        FilteredQueryBuilder filteredQuery = QueryBuilders.filteredQuery(propertyQuery, null);
        return QueryBuilders.nestedQuery(ElasticSearchNestedSearchIndex.PROPERTY_NESTED_FIELD_NAME, filteredQuery);
    }

    private String[] splitStringIntoTerms(String value) {
        String[] values = value.split("[ -]");
        for (int i = 0; i < values.length; i++) {
            values[i] = values[i].trim();
        }
        return values;
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

    @Override
    public void addFacet(Facet facet) {
        this.facets.add(facet);
    }
}
