package org.securegraph.elasticsearch;

import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.securegraph.*;
import org.securegraph.property.StreamingPropertyValue;
import org.securegraph.query.GraphQuery;
import org.securegraph.type.GeoPoint;
import org.securegraph.util.StreamUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

public class ElasticSearchParentChildSearchIndex extends ElasticSearchSearchIndexBase {
    private static final Logger LOGGER = LoggerFactory.getLogger(ElasticSearchParentChildSearchIndex.class);
    public static final String PROPERTY_TYPE = "property";

    public ElasticSearchParentChildSearchIndex(Map config) {
        super(config);
    }

    @Override
    protected void createIndex(boolean storeSourceData) throws IOException {
        super.createIndex(storeSourceData);
        XContentBuilder mapping = XContentFactory.jsonBuilder()
                .startObject()
                .startObject(PROPERTY_TYPE)
                .startObject("_parent").field("type", ELEMENT_TYPE).endObject()
                .startObject("_source").field("enabled", storeSourceData).endObject()
                .startObject("properties")
                .startObject(VISIBILITY_FIELD_NAME).field("type", "string").field("store", "true").endObject()
                .endObject()
                .endObject()
                .endObject();
        LOGGER.debug(mapping.string());
        PutMappingResponse response = getClient()
                .admin()
                .indices()
                .preparePutMapping(getIndexName())
                .setIgnoreConflicts(false)
                .setType(PROPERTY_TYPE)
                .setSource(mapping)
                .execute()
                .actionGet();
        LOGGER.debug(response.toString());
    }

    @Override
    public void addElement(Graph graph, Element element, Authorizations authorizations) {
        addPropertiesToIndex(element.getProperties());

        try {
            try {
                addParentDocument(graph, element, authorizations);
            } catch (Exception ex) {
                throw new SecureGraphException("Could not add parent document", ex);
            }

            for (Property property : element.getProperties()) {
                try {
                    addPropertyDocument(graph, element, property, authorizations);
                } catch (Exception ex) {
                    throw new SecureGraphException("Could not add property: " + property, ex);
                }
            }

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

    private void addPropertyDocument(Graph graph, Element element, Property property, Authorizations authorizations) throws IOException {
        XContentBuilder jsonBuilder = buildJsonContentFromProperty(graph, element, property, authorizations);
        if (jsonBuilder == null) {
            return;
        }

        String id = element.getId().toString() + "_" + property.getName() + "_" + property.getKey();

        LOGGER.debug(jsonBuilder.string());
        IndexRequestBuilder builder = getClient().prepareIndex(getIndexName(), PROPERTY_TYPE, id);
        builder = builder.setParent(element.getId().toString());
        builder = builder.setSource(jsonBuilder);
        IndexResponse response = builder.execute().actionGet();
        if (response.getId() == null) {
            throw new SecureGraphException("Could not index document " + element.getId());
        }
    }

    private void addParentDocument(Graph graph, Element element, Authorizations authorizations) throws IOException {
        XContentBuilder jsonBuilder;
        jsonBuilder = XContentFactory.jsonBuilder()
                .startObject();

        String id = element.getId().toString();
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

        IndexResponse response = getClient()
                .prepareIndex(getIndexName(), ELEMENT_TYPE, id)
                .setSource(jsonBuilder.endObject())
                .execute()
                .actionGet();
        if (response.getId() == null) {
            throw new SecureGraphException("Could not index document " + element.getId());
        }
    }

    private XContentBuilder buildJsonContentFromProperty(Graph graph, Element element, Property property, Authorizations authorizations) throws IOException {
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
            propertyValue = propertyValueMap;
        } else if (propertyValue instanceof StreamingPropertyValue) {
            StreamingPropertyValue streamingPropertyValue = (StreamingPropertyValue) propertyValue;
            if (!streamingPropertyValue.isSearchIndex()) {
                return null;
            }
            Class valueType = streamingPropertyValue.getValueType();
            if (valueType == String.class) {
                InputStream in = streamingPropertyValue.getInputStream();
                propertyValue = StreamUtils.toString(in);
            } else {
                throw new SecureGraphException("Unhandled StreamingPropertyValue type: " + valueType.getName());
            }
        } else if (propertyValue instanceof String) {
            PropertyDefinition propertyDefinition = getPropertyDefinitions().get(property.getName());
            if (propertyDefinition == null || propertyDefinition.getTextIndexHints().contains(TextIndexHint.EXACT_MATCH)) {
                jsonBuilder.field(property.getName() + ElasticSearchSearchIndexBase.EXACT_MATCH_PROPERTY_NAME_SUFFIX, propertyValue);
            }
            if (propertyDefinition == null || propertyDefinition.getTextIndexHints().contains(TextIndexHint.FULL_TEXT)) {
                jsonBuilder.field(property.getName(), propertyValue);
            }
        }

        if (propertyValue instanceof DateOnly) {
            propertyValue = ((DateOnly) propertyValue).getDate();
        }

        jsonBuilder.field(property.getName(), propertyValue);
        jsonBuilder.field(VISIBILITY_FIELD_NAME, property.getVisibility().getVisibilityString());

        return jsonBuilder;
    }

    @Override
    public GraphQuery queryGraph(Graph graph, String queryString, Authorizations authorizations) {
        return new ElasticSearchParentChildGraphQuery(getClient(), getIndexName(), graph, queryString, getPropertyDefinitions(), getInEdgeBoost(), getOutEdgeBoost(), authorizations);
    }

    @Override
    protected void addPropertyToIndex(String propertyName, Class dataType, boolean analyzed, Double boost) throws IOException {
        if (getPropertyDefinitions().get(propertyName) != null) {
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
                .startObject(propertyName);

        addTypeToMapping(mapping, propertyName, dataType, analyzed, boost);

        mapping
                .endObject()
                .endObject()
                .endObject()
                .endObject();

        PutMappingResponse response = getClient()
                .admin()
                .indices()
                .preparePutMapping(getIndexName())
                .setIgnoreConflicts(false)
                .setType(PROPERTY_TYPE)
                .setSource(mapping)
                .execute()
                .actionGet();
        LOGGER.debug(response.toString());

        getPropertyDefinitions().put(propertyName, new PropertyDefinition(propertyName, dataType, TextIndexHint.ALL));
    }

    @Override
    public boolean isEdgeBoostSupported() {
        return false;
    }
}
