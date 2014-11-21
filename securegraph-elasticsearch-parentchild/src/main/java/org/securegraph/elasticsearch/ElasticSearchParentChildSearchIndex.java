package org.securegraph.elasticsearch;

import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.deletebyquery.DeleteByQueryResponse;
import org.elasticsearch.action.deletebyquery.IndexDeleteByQueryResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.*;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;
import org.securegraph.*;
import org.securegraph.property.MutablePropertyImpl;
import org.securegraph.property.StreamingPropertyValue;
import org.securegraph.query.GraphQuery;
import org.securegraph.type.GeoPoint;
import org.securegraph.util.StreamUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

import static org.securegraph.util.IterableUtils.toArray;

public class ElasticSearchParentChildSearchIndex extends ElasticSearchSearchIndexBase {
    private static final Logger LOGGER = LoggerFactory.getLogger(ElasticSearchParentChildSearchIndex.class);
    public static final String PROPERTY_TYPE = "property";
    public static final int BATCH_SIZE = 1000;

    public ElasticSearchParentChildSearchIndex(Map config) {
        super(config);
    }

    @Override
    protected void ensureMappingsCreated(IndexInfo indexInfo) {
        ParentChildIndexInfo parentChildIndexInfo = (ParentChildIndexInfo) indexInfo;
        super.ensureMappingsCreated(indexInfo);

        if (!parentChildIndexInfo.isPropertyTypeDefined()) {
            try {
                XContentBuilder mappingBuilder = XContentFactory.jsonBuilder()
                        .startObject()
                        .startObject("_parent").field("type", ELEMENT_TYPE).endObject()
                        .startObject("_source").field("enabled", isStoreSourceData()).endObject()
                        .startObject("properties")
                        .startObject(VISIBILITY_FIELD_NAME)
                        .field("type", "string")
                        .field("analyzer", "keyword")
                        .field("index", "not_analyzed")
                        .field("store", "true")
                        .endObject();
                XContentBuilder mapping = mappingBuilder.endObject()
                        .endObject();

                PutMappingResponse putMappingResponse = getClient().admin().indices().preparePutMapping(indexInfo.getIndexName())
                        .setIgnoreConflicts(false)
                        .setType(PROPERTY_TYPE)
                        .setSource(mapping)
                        .execute()
                        .actionGet();
                LOGGER.debug(putMappingResponse.toString());
                parentChildIndexInfo.setPropertyTypeDefined(true);
            } catch (IOException e) {
                throw new SecureGraphException("Could not add mappings to index: " + indexInfo.getIndexName(), e);
            }
        }
    }

    @Override
    protected IndexInfo createIndexInfo(String indexName) {
        return new ParentChildIndexInfo(indexName);
    }

    @Override
    protected void createIndexAddFieldsToElementType(XContentBuilder builder) throws IOException {
        super.createIndexAddFieldsToElementType(builder);
        builder.startObject(VISIBILITY_FIELD_NAME).field("type", "string").field("analyzer", "keyword").field("index", "not_analyzed").field("store", "true").endObject();
    }

    @Override
    public void removeElement(Graph graph, Element element, Authorizations authorizations) {
        String indexName = getIndexName(element);
        deleteChildDocuments(indexName, element);
        deleteParentDocument(indexName, element);
    }

    private void deleteChildDocuments(String indexName, Element element) {
        String parentId = element.getId();
        DeleteByQueryResponse response = getClient()
                .prepareDeleteByQuery(indexName)
                .setTypes(PROPERTY_TYPE)
                .setQuery(
                        QueryBuilders.termQuery("_parent", ELEMENT_TYPE + "#" + parentId)
                )
                .execute()
                .actionGet();
        if (response.status() != RestStatus.OK) {
            throw new SecureGraphException("Could not remove child elements " + element.getId() + " (status: " + response.status() + ")");
        }
        if (LOGGER.isDebugEnabled()) {
            for (IndexDeleteByQueryResponse r : response) {
                LOGGER.debug("deleted child document " + r.toString());
            }
        }
    }

    private void deleteParentDocument(String indexName, Element element) {
        String id = element.getId();
        LOGGER.debug("deleting parent document " + id);
        DeleteResponse deleteResponse = getClient().delete(
                getClient()
                        .prepareDelete(indexName, ELEMENT_TYPE, id)
                        .request()
        ).actionGet();
        if (!deleteResponse.isFound()) {
            LOGGER.warn("Could not remove element " + element.getId());
        }
    }

    @Override
    public void removeProperty(Graph graph, Element element, Property property, Authorizations authorizations) {
        String indexName = getIndexName(element);
        String id = getChildDocId(element, property);
        DeleteResponse deleteResponse = getClient().delete(
                getClient()
                        .prepareDelete(indexName, PROPERTY_TYPE, id)
                        .request()
        ).actionGet();
        if (!deleteResponse.isFound()) {
            LOGGER.warn("Could not remove property " + element.getId() + " " + property.toString());
        }
        LOGGER.debug("deleted property " + element.getId() + " " + property.toString());
    }

    @Override
    public void addElement(Graph graph, Element element, Authorizations authorizations) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("addElement: " + element.getId());
        }
        if (!isIndexEdges() && element instanceof Edge) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("skipping edge: " + element.getId());
            }
            return;
        }

        IndexInfo indexInfo = addPropertiesToIndex(element, element.getProperties());

        try {
            BulkRequest bulkRequest = new BulkRequest();

            addElementToBulkRequest(indexInfo, bulkRequest, element, authorizations);

            doBulkRequest(bulkRequest);

            if (isAutoflush()) {
                flush();
            }
        } catch (Exception e) {
            throw new SecureGraphException("Could not add element", e);
        }

        if (isUseEdgeBoost() && element instanceof Edge) {
            Element vOut = ((Edge) element).getVertex(Direction.OUT, authorizations);
            if (vOut != null) {
                addElement(graph, vOut, authorizations);
            }
            Element vIn = ((Edge) element).getVertex(Direction.IN, authorizations);
            if (vIn != null) {
                addElement(graph, vIn, authorizations);
            }
        }
    }

    @Override
    public void addElements(Graph graph, Iterable<Element> elements, Authorizations authorizations) {
        int totalCount = 0;
        Map<IndexInfo, BulkRequestWithCount> bulkRequests = new HashMap<IndexInfo, BulkRequestWithCount>();
        for (Element element : elements) {
            String indexName = getIndexName(element);
            IndexInfo indexInfo = ensureIndexCreatedAndInitialized(indexName, isStoreSourceData());
            BulkRequestWithCount bulkRequestWithCount = bulkRequests.get(indexInfo);
            if (bulkRequestWithCount == null) {
                bulkRequestWithCount = new BulkRequestWithCount();
                bulkRequests.put(indexInfo, bulkRequestWithCount);
            }

            if (bulkRequestWithCount.getCount() >= BATCH_SIZE) {
                LOGGER.debug("adding elements... " + totalCount);
                doBulkRequest(bulkRequestWithCount.getBulkRequest());
                bulkRequestWithCount.clear();
            }
            addElementToBulkRequest(indexInfo, bulkRequestWithCount.getBulkRequest(), element, authorizations);
            bulkRequestWithCount.incrementCount();
            totalCount++;

            if (isUseEdgeBoost() && element instanceof Edge) {
                Element vOut = ((Edge) element).getVertex(Direction.OUT, authorizations);
                if (vOut != null) {
                    addElementToBulkRequest(indexInfo, bulkRequestWithCount.getBulkRequest(), vOut, authorizations);
                    bulkRequestWithCount.incrementCount();
                    totalCount++;
                }
                Element vIn = ((Edge) element).getVertex(Direction.IN, authorizations);
                if (vIn != null) {
                    addElementToBulkRequest(indexInfo, bulkRequestWithCount.getBulkRequest(), vIn, authorizations);
                    bulkRequestWithCount.incrementCount();
                    totalCount++;
                }
            }
        }
        for (BulkRequestWithCount bulkRequestWithCount : bulkRequests.values()) {
            if (bulkRequestWithCount.getCount() > 0) {
                doBulkRequest(bulkRequestWithCount.getBulkRequest());
            }
        }
        LOGGER.debug("added " + totalCount + " elements");

        if (isAutoflush()) {
            flush();
        }
    }

    private void addElementToBulkRequest(IndexInfo indexInfo, BulkRequest bulkRequest, Element element, Authorizations authorizations) {
        try {
            bulkRequest.add(getParentDocumentIndexRequest(indexInfo, element, authorizations));
            for (Property property : element.getProperties()) {
                IndexRequest propertyIndexRequest = getPropertyDocumentIndexRequest(indexInfo, element, property);
                if (propertyIndexRequest != null) {
                    bulkRequest.add(propertyIndexRequest);
                }
            }
        } catch (IOException ex) {
            throw new SecureGraphException("Could not add element to bulk request", ex);
        }
    }

    public IndexRequest getPropertyDocumentIndexRequest(Element element, Property property) throws IOException {
        String indexName = getIndexName(element);
        IndexInfo indexInfo = ensureIndexCreatedAndInitialized(indexName, isStoreSourceData());
        return getPropertyDocumentIndexRequest(indexInfo, element, property);
    }

    private IndexRequest getPropertyDocumentIndexRequest(IndexInfo indexInfo, Element element, Property property) throws IOException {
        XContentBuilder jsonBuilder = buildJsonContentFromProperty(indexInfo, property);
        if (jsonBuilder == null) {
            return null;
        }

        String id = getChildDocId(element, property);

        //LOGGER.debug(jsonBuilder.string());
        IndexRequestBuilder builder = getClient().prepareIndex(indexInfo.getIndexName(), PROPERTY_TYPE, id);
        builder = builder.setParent(element.getId());
        builder = builder.setSource(jsonBuilder);
        return builder.request();
    }

    private String getChildDocId(Element element, Property property) {
        return element.getId() + "_" + property.getName() + "_" + property.getKey();
    }

    public IndexRequest getParentDocumentIndexRequest(Element element, Authorizations authorizations) throws IOException {
        String indexName = getIndexName(element);
        IndexInfo indexInfo = ensureIndexCreatedAndInitialized(indexName, isStoreSourceData());
        return getParentDocumentIndexRequest(indexInfo, element, authorizations);
    }

    private IndexRequest getParentDocumentIndexRequest(IndexInfo indexInfo, Element element, Authorizations authorizations) throws IOException {
        XContentBuilder jsonBuilder;
        jsonBuilder = XContentFactory.jsonBuilder()
                .startObject();

        String id = element.getId();
        if (element instanceof Vertex) {
            jsonBuilder.field(ElasticSearchSearchIndexBase.ELEMENT_TYPE_FIELD_NAME, ElasticSearchSearchIndexBase.ELEMENT_TYPE_VERTEX);
            if (isUseEdgeBoost()) {
                int inEdgeCount = ((Vertex) element).getEdgeCount(Direction.IN, authorizations);
                jsonBuilder.field(ElasticSearchSearchIndexBase.IN_EDGE_COUNT_FIELD_NAME, inEdgeCount);
                int outEdgeCount = ((Vertex) element).getEdgeCount(Direction.OUT, authorizations);
                jsonBuilder.field(ElasticSearchSearchIndexBase.OUT_EDGE_COUNT_FIELD_NAME, outEdgeCount);
            }
        } else if (element instanceof Edge) {
            jsonBuilder.field(ElasticSearchSearchIndexBase.ELEMENT_TYPE_FIELD_NAME, ElasticSearchSearchIndexBase.ELEMENT_TYPE_EDGE);
        } else {
            throw new SecureGraphException("Unexpected element type " + element.getClass().getName());
        }

        jsonBuilder.field(VISIBILITY_FIELD_NAME, element.getVisibility().getVisibilityString());

        return new IndexRequest(indexInfo.getIndexName(), ELEMENT_TYPE, id).source(jsonBuilder);
    }

    private XContentBuilder buildJsonContentFromProperty(IndexInfo indexInfo, Property property) throws IOException {
        XContentBuilder jsonBuilder;
        jsonBuilder = XContentFactory.jsonBuilder()
                .startObject();

        Object propertyValue = property.getValue();
        if (propertyValue != null && shouldIgnoreType(propertyValue.getClass())) {
            return null;
        } else if (propertyValue instanceof GeoPoint) {
            GeoPoint geoPoint = (GeoPoint) propertyValue;
            Map<String, Object> propertyValueMap = new HashMap<String, Object>();
            propertyValueMap.put("lat", geoPoint.getLatitude());
            propertyValueMap.put("lon", geoPoint.getLongitude());

            jsonBuilder.field(property.getName() + ElasticSearchSearchIndexBase.GEO_PROPERTY_NAME_SUFFIX, propertyValueMap);
            if (geoPoint.getDescription() != null) {
                jsonBuilder.field(property.getName(), geoPoint.getDescription());
            }
        } else if (propertyValue instanceof StreamingPropertyValue) {
            StreamingPropertyValue streamingPropertyValue = (StreamingPropertyValue) propertyValue;
            if (!streamingPropertyValue.isSearchIndex()) {
                return null;
            }
            Class valueType = streamingPropertyValue.getValueType();
            if (valueType == String.class) {
                InputStream in = streamingPropertyValue.getInputStream();
                propertyValue = StreamUtils.toString(in);
                jsonBuilder.field(property.getName(), propertyValue);
            } else {
                throw new SecureGraphException("Unhandled StreamingPropertyValue type: " + valueType.getName());
            }
        } else if (propertyValue instanceof String) {
            PropertyDefinition propertyDefinition = indexInfo.getPropertyDefinitions().get(property.getName());
            if (propertyDefinition == null || propertyDefinition.getTextIndexHints().contains(TextIndexHint.EXACT_MATCH)) {
                jsonBuilder.field(property.getName() + ElasticSearchSearchIndexBase.EXACT_MATCH_PROPERTY_NAME_SUFFIX, propertyValue);
            }
            if (propertyDefinition == null || propertyDefinition.getTextIndexHints().contains(TextIndexHint.FULL_TEXT)) {
                jsonBuilder.field(property.getName(), propertyValue);
            }
        } else {
            if (propertyValue instanceof DateOnly) {
                propertyValue = ((DateOnly) propertyValue).getDate();
            }

            jsonBuilder.field(property.getName(), propertyValue);
        }
        jsonBuilder.field(VISIBILITY_FIELD_NAME, property.getVisibility().getVisibilityString());

        return jsonBuilder;
    }

    @Override
    public GraphQuery queryGraph(Graph graph, String queryString, Authorizations authorizations) {
        return new ElasticSearchParentChildGraphQuery(getClient(), getIndicesToQuery(), graph, queryString, getAllPropertyDefinitions(), getInEdgeBoost(), getOutEdgeBoost(), authorizations);
    }

    @Override
    protected void addPropertyToIndex(IndexInfo indexInfo, String propertyName, Class dataType, boolean analyzed, Double boost) throws IOException {
        if (indexInfo.isPropertyDefined(propertyName)) {
            return;
        }

        if (shouldIgnoreType(dataType)) {
            return;
        }

        XContentBuilder mapping = XContentFactory.jsonBuilder()
                .startObject()
                .startObject(PROPERTY_TYPE)
                .startObject("_parent").field("type", ELEMENT_TYPE).endObject()
                .startObject("properties")
                .startObject(propertyName)
                .field("store", isStoreSourceData());

        addTypeToMapping(mapping, propertyName, dataType, analyzed, boost);

        mapping
                .endObject()
                .endObject()
                .endObject()
                .endObject();

        PutMappingResponse response = getClient()
                .admin()
                .indices()
                .preparePutMapping(indexInfo.getIndexName())
                .setIgnoreConflicts(false)
                .setType(PROPERTY_TYPE)
                .setSource(mapping)
                .execute()
                .actionGet();
        LOGGER.debug(response.toString());

        indexInfo.addPropertyDefinition(propertyName, new PropertyDefinition(propertyName, dataType, TextIndexHint.ALL));
    }

    @Override
    public Map<String, VertexQueryResult> getVertex(Iterable<String> vertexIds, EnumSet<FetchHint> fetchHints, Authorizations authorizations) {
        String[] vertexIdsArray = toArray(vertexIds, String.class);

        FilterBuilder parentFilter = FilterBuilders.idsFilter(ElasticSearchSearchIndexBase.ELEMENT_TYPE).ids(vertexIdsArray);
        FilterBuilder childFilter = FilterBuilders.hasParentFilter(
                ElasticSearchSearchIndexBase.ELEMENT_TYPE,
                FilterBuilders.idsFilter(ElasticSearchSearchIndexBase.ELEMENT_TYPE).ids(vertexIdsArray));

        OrFilterBuilder f = FilterBuilders.orFilter(parentFilter, childFilter);
        QueryBuilder query = QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(), f);

        SearchRequestBuilder searchRequestBuilder = getClient()
                .prepareSearch()
                .addFields("_parent", "_source")
                .setQuery(query);
        LOGGER.debug("query: " + searchRequestBuilder);

        SearchResponse searchResults = getClient().search(searchRequestBuilder.request()).actionGet();
        SearchHit[] hits = searchResults.getHits().hits();
        if (hits.length == 0) {
            return null;
        }

        Map<String, VertexQueryResult> results = new HashMap<String, VertexQueryResult>();
        for (SearchHit hit : hits) {
            SearchHitField parentField = hit.getFields() == null ? null : hit.getFields().get("_parent");
            String vertexId;
            if (parentField == null) {
                // parent document
                vertexId = hit.getId();
            } else {
                // child document
                vertexId = parentField.value();
            }

            VertexQueryResult result = results.get(vertexId);
            if (result == null) {
                result = new VertexQueryResult(vertexId);
                results.put(vertexId, result);
            }

            if (parentField == null) {
                // parent document
                String vertexVisibilityString = (String) hit.getSource().get(VISIBILITY_FIELD_NAME);
                Visibility vertexVisibility = new Visibility(vertexVisibilityString);
                result.setVertexVisibility(vertexVisibility);
            } else {
                // child document
                String propertyVisibilityString = null;
                String propertyName = null;
                Object propertyValue = null;
                for (Map.Entry<String, Object> sourceEntry : hit.getSource().entrySet()) {
                    String entryName = sourceEntry.getKey();
                    if (entryName.equals(VISIBILITY_FIELD_NAME)) {
                        propertyVisibilityString = (String) sourceEntry.getValue();
                    } else if (entryName.endsWith(EXACT_MATCH_PROPERTY_NAME_SUFFIX)) {
                        propertyName = entryName.substring(0, entryName.length() - EXACT_MATCH_PROPERTY_NAME_SUFFIX.length());
                        propertyValue = sourceEntry.getValue();
                    } else {
                        propertyName = entryName;
                        propertyValue = sourceEntry.getValue();
                    }
                }

                if (propertyVisibilityString == null) {
                    throw new SecureGraphException("Could not find " + VISIBILITY_FIELD_NAME + " field on element " + vertexId);
                }
                if (propertyName == null) {
                    throw new SecureGraphException("Could not find property name and value on element " + vertexId);
                }

                String key = ""; // TODO fill me in
                Map<String, Object> metadata = new HashMap<String, Object>(); // TODO fill me in
                Visibility visibility = new Visibility(propertyVisibilityString);
                Property property = new MutablePropertyImpl(key, propertyName, propertyValue, metadata, visibility);
                result.getProperties().add(property);
            }
        }

        return results;
    }

    @Override
    public boolean isEdgeBoostSupported() {
        return false;
    }

    @Override
    public SearchIndexSecurityGranularity getSearchIndexSecurityGranularity() {
        return SearchIndexSecurityGranularity.PROPERTY;
    }
}
