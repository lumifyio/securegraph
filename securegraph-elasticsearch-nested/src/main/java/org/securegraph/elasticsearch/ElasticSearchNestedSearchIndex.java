package org.securegraph.elasticsearch;

import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.admin.indices.status.IndicesStatusResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.securegraph.*;
import org.securegraph.property.StreamingPropertyValue;
import org.securegraph.query.DefaultVertexQuery;
import org.securegraph.query.GraphQuery;
import org.securegraph.query.VertexQuery;
import org.securegraph.search.SearchIndex;
import org.securegraph.type.GeoPoint;
import org.securegraph.util.StreamUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class ElasticSearchNestedSearchIndex implements SearchIndex {
    private static final Logger LOGGER = LoggerFactory.getLogger(ElasticSearchNestedSearchIndex.class);
    private static final String STORE_SOURCE_DATA = "storeSourceData";
    public static final String ES_LOCATIONS = "locations";
    public static final String INDEX_NAME = "indexName";
    private static final String DEFAULT_INDEX_NAME = "securegraph";
    public static final String ELEMENT_TYPE = "element";
    public static final String ELEMENT_TYPE_FIELD_NAME = "__elementType";
    public static final String PROPERTY_TEXT_FIELD_NAME = "__text";
    public static final String PROPERTY_VISIBILITY_FIELD_NAME = "__visibility";
    public static final String ELEMENT_TYPE_VERTEX = "vertex";
    public static final String ELEMENT_TYPE_EDGE = "edge";
    public static final int DEFAULT_ES_PORT = 9300;
    public static final String EXACT_MATCH_PROPERTY_NAME_SUFFIX = "_exactMatch";
    public static final String PROPERTY_NESTED_FIELD_NAME = "property";
    public static final String SETTING_CLUSTER_NAME = "clusterName";
    private final TransportClient client;
    private final boolean autoflush;
    private String indexName;
    private Map<String, PropertyDefinition> propertyDefinitions = new HashMap<String, PropertyDefinition>();

    public ElasticSearchNestedSearchIndex(Map config) {
        String esLocationsString = (String) config.get(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + ES_LOCATIONS);
        if (esLocationsString == null) {
            throw new SecureGraphException(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + ES_LOCATIONS + " is a required configuration parameter");
        }
        LOGGER.info("Using elastic search locations: " + esLocationsString);
        String[] esLocations = esLocationsString.split(",");

        indexName = (String) config.get(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + INDEX_NAME);
        if (indexName == null) {
            indexName = DEFAULT_INDEX_NAME;
        }
        LOGGER.info("Using index: " + indexName);

        Object storeSourceDataConfig = config.get(STORE_SOURCE_DATA);
        boolean storeSourceData = storeSourceDataConfig != null && "true".equals(storeSourceDataConfig.toString());
        LOGGER.info("Store source data: " + storeSourceData);

        // TODO convert this to use a proper config object
        Object autoFlushObj = config.get(GraphConfiguration.AUTO_FLUSH);
        autoflush = autoFlushObj != null && "true".equals(autoFlushObj.toString());
        LOGGER.info("Auto flush: " + autoflush);

        ImmutableSettings.Builder settingsBuilder = ImmutableSettings.settingsBuilder();
        if (config.get(SETTING_CLUSTER_NAME) != null) {
            settingsBuilder.put("cluster.name", config.get(SETTING_CLUSTER_NAME));
        }
        client = new TransportClient(settingsBuilder.build());
        for (String esLocation : esLocations) {
            String[] locationSocket = esLocation.split(":");
            String hostname;
            int port;
            if (locationSocket.length == 2) {
                hostname = locationSocket[0];
                port = Integer.parseInt(locationSocket[1]);
            } else if (locationSocket.length == 1) {
                hostname = locationSocket[0];
                port = DEFAULT_ES_PORT;
            } else {
                throw new SecureGraphException("Invalid elastic search location: " + esLocation);
            }
            client.addTransportAddress(new InetSocketTransportAddress(hostname, port));
        }

        if (!client.admin().indices().prepareExists(indexName).execute().actionGet().isExists()) {
            try {
                XContentBuilder mapping = XContentFactory.jsonBuilder()
                        .startObject()
                        .startObject(ELEMENT_TYPE)
                        .startObject("_source")
                        .field("enabled", storeSourceData)
                        .endObject()
                        .startObject("properties")
                        .startObject(ELEMENT_TYPE_FIELD_NAME)
                        .field("type", "string")
                        .endObject()
                        .startObject(PROPERTY_NESTED_FIELD_NAME)
                        .field("type", "nested")
                        .field("include_in_parent", false)
                        .startObject("properties")
                        .startObject(PROPERTY_TEXT_FIELD_NAME)
                        .field("type", "string")
                        .endObject()
                        .startObject(PROPERTY_VISIBILITY_FIELD_NAME)
                        .field("type", "string")
                        .field("include_in_all", false)
                        .field("store", true)
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject();
                CreateIndexResponse createResponse = client.admin().indices().prepareCreate(indexName).addMapping(ELEMENT_TYPE, mapping).execute().actionGet();
                LOGGER.debug(createResponse.toString());

                IndicesStatusResponse statusResponse = client.admin().indices().prepareStatus(indexName).execute().actionGet();
                LOGGER.debug(statusResponse.toString());
            } catch (IOException e) {
                throw new SecureGraphException("Could not create index", e);
            }
        }
    }

    @Override
    public void addElement(Graph graph, Element element) {
        addPropertiesToIndex(element.getProperties());

        try {
            XContentBuilder jsonBuilder = buildJsonContentFromElement(element);

            IndexResponse response = client
                    .prepareIndex(indexName, ELEMENT_TYPE, element.getId().toString())
                    .setSource(jsonBuilder.endObject())
                    .execute()
                    .actionGet();
            if (response.getId() == null) {
                throw new SecureGraphException("Could not index document " + element.getId());
            }

            if (autoflush) {
                flush();
            }
        } catch (Exception e) {
            throw new SecureGraphException("Could not add document", e);
        }
    }

    public String createJsonForElement(Element element) {
        try {
            return buildJsonContentFromElement(element).string();
        } catch (Exception e) {
            throw new SecureGraphException("Could not create JSON for element", e);
        }
    }

    private XContentBuilder buildJsonContentFromElement(Element element) throws IOException {
        XContentBuilder jsonBuilder;
        jsonBuilder = XContentFactory.jsonBuilder().startObject();

        if (element instanceof Vertex) {
            jsonBuilder.field(ELEMENT_TYPE_FIELD_NAME, ELEMENT_TYPE_VERTEX);
        } else if (element instanceof Edge) {
            jsonBuilder.field(ELEMENT_TYPE_FIELD_NAME, ELEMENT_TYPE_EDGE);
        } else {
            throw new SecureGraphException("Unexpected element type " + element.getClass().getName());
        }

        jsonBuilder.startArray("property");

        for (Property property : element.getProperties()) {
            Object propertyValue = property.getValue();
            String propertyName = property.getName();

            if (propertyValue != null && shouldIgnoreType(propertyValue.getClass())) {
                continue;
            } else if (propertyValue instanceof GeoPoint) {
                GeoPoint geoPoint = (GeoPoint) propertyValue;
                Map<String, Object> propertyValueMap = new HashMap<String, Object>();
                propertyValueMap.put("type", "point");
                propertyValueMap.put("coordinates", new double[]{geoPoint.getLongitude(), geoPoint.getLatitude()});
                propertyValue = propertyValueMap;
            } else if (propertyValue instanceof StreamingPropertyValue) {
                StreamingPropertyValue streamingPropertyValue = (StreamingPropertyValue) propertyValue;
                if (!streamingPropertyValue.isSearchIndex()) {
                    continue;
                }
                Class valueType = streamingPropertyValue.getValueType();
                if (valueType == String.class) {
                    InputStream in = streamingPropertyValue.getInputStream();
                    propertyValue = StreamUtils.toString(in);
                } else {
                    throw new SecureGraphException("Unhandled StreamingPropertyValue type: " + valueType.getName());
                }
            } else if (propertyValue instanceof String) {
                PropertyDefinition propertyDefinition = this.propertyDefinitions.get(propertyName);
                if (propertyDefinition == null || propertyDefinition.getTextIndexHints().contains(TextIndexHint.EXACT_MATCH)) {
                    propertyName = property.getName() + EXACT_MATCH_PROPERTY_NAME_SUFFIX;
                    addField(jsonBuilder, propertyName, propertyValue, property.getVisibility().toString());
                }
                if (propertyDefinition == null || propertyDefinition.getTextIndexHints().contains(TextIndexHint.FULL_TEXT)) {
                    propertyName = property.getName();
                    addField(jsonBuilder, propertyName, propertyValue, property.getVisibility().toString());
                }
                continue;
            }

            if (propertyValue instanceof DateOnly) {
                propertyValue = ((DateOnly) propertyValue).getDate();
            }

            addField(jsonBuilder, propertyName, propertyValue, property.getVisibility().toString());
        }

        jsonBuilder.endArray();

        return jsonBuilder;
    }

    private void addField(XContentBuilder jsonBuilder, String propertyName, Object propertyValue, String visibility) throws IOException {
        jsonBuilder.startObject()
                .field(propertyName, propertyValue)
                .field(PROPERTY_VISIBILITY_FIELD_NAME, visibility);

        if (propertyValue != null) {
            jsonBuilder.field(PROPERTY_TEXT_FIELD_NAME, propertyValue.toString());
        }

        jsonBuilder.endObject();
    }

    @Override
    public void flush() {
        client.admin().indices().prepareFlush(indexName).execute().actionGet();
    }

    @Override
    public void shutdown() {
        client.close();
    }

    @Override
    public void addPropertyDefinition(PropertyDefinition propertyDefinition) {
        this.propertyDefinitions.put(propertyDefinition.getPropertyName(), propertyDefinition);
    }

    public void addPropertiesToIndex(Iterable<Property> properties) {
        try {
            for (Property property : properties) {
                addPropertyToIndex(property);
            }
        } catch (IOException e) {
            throw new SecureGraphException("Could not add properties to index", e);
        }
    }

    public void addPropertyToIndex(Property property) throws IOException {
        String propertyName = property.getName();

        if (propertyDefinitions.get(propertyName) != null) {
            return;
        }

        Class dataType;
        Object propertyValue = property.getValue();
        if (propertyValue instanceof StreamingPropertyValue) {
            StreamingPropertyValue streamingPropertyValue = (StreamingPropertyValue) propertyValue;
            if (!streamingPropertyValue.isSearchIndex()) {
                return;
            }
            dataType = streamingPropertyValue.getValueType();
        } else {
            dataType = propertyValue.getClass();
        }

        addPropertyToIndex(propertyName, dataType, true);
    }

    private void addPropertyToIndex(String propertyName, Class dataType, boolean analyzed) throws IOException {
        if (propertyDefinitions.get(propertyName) != null) {
            return;
        }

        if (shouldIgnoreType(dataType)) {
            return;
        }

        XContentBuilder mapping = XContentFactory.jsonBuilder()
                .startObject()
                .startObject(ELEMENT_TYPE)
                .startObject("properties")
                .startObject(PROPERTY_NESTED_FIELD_NAME)
                .field("type", "nested")
                .field("include_in_parent", false)
                .startObject("properties")
                .startObject(propertyName);

        if (dataType == String.class) {
            LOGGER.debug("Registering string type for {}", propertyName);
            mapping.field("type", "string");
            if (!analyzed) {
                mapping.field("index", "not_analyzed");
            }
        } else if (dataType == Float.class) {
            LOGGER.debug("Registering float type for {}", propertyName);
            mapping.field("type", "float");
        } else if (dataType == Double.class) {
            LOGGER.debug("Registering double type for {}", propertyName);
            mapping.field("type", "double");
        } else if (dataType == Byte.class) {
            LOGGER.debug("Registering byte type for {}", propertyName);
            mapping.field("type", "byte");
        } else if (dataType == Short.class) {
            LOGGER.debug("Registering short type for {}", propertyName);
            mapping.field("type", "short");
        } else if (dataType == Integer.class) {
            LOGGER.debug("Registering integer type for {}", propertyName);
            mapping.field("type", "integer");
        } else if (dataType == Date.class || dataType == DateOnly.class) {
            LOGGER.debug("Registering date type for {}", propertyName);
            mapping.field("type", "date");
        } else if (dataType == Long.class) {
            LOGGER.debug("Registering long type for {}", propertyName);
            mapping.field("type", "long");
        } else if (dataType == Boolean.class) {
            LOGGER.debug("Registering boolean type for {}", propertyName);
            mapping.field("type", "boolean");
        } else if (dataType == GeoPoint.class) {
            LOGGER.debug("Registering geo_shape type for {}", propertyName);
            mapping.field("type", "geo_shape");
        } else {
            throw new SecureGraphException("Unexpected value type for property \"" + propertyName + "\": " + dataType.getName());
        }

        mapping
                .endObject()
                .endObject()
                .endObject()
                .endObject()
                .endObject()
                .endObject();

        PutMappingResponse response = client
                .admin()
                .indices()
                .preparePutMapping(indexName)
                .setIgnoreConflicts(false)
                .setType(ELEMENT_TYPE)
                .setSource(mapping)
                .execute()
                .actionGet();
        LOGGER.debug(response.toString());

        propertyDefinitions.put(propertyName, new PropertyDefinition(propertyName, dataType, TextIndexHint.NONE));
    }

    protected boolean shouldIgnoreType(Class dataType) {
        if (dataType == byte[].class) {
            return true;
        }
        return false;
    }

    @Override
    public void removeElement(Graph graph, Element element) {
        // TODO add test to verify this
        Object id = element.getId();
        DeleteResponse response = client.prepareDelete(indexName, ELEMENT_TYPE, id.toString()).execute().actionGet();
        LOGGER.debug(response.toString());
    }

    @Override
    public void addElements(Graph graph, Iterable<Element> elements) {
        // TODO change this to use elastic search bulk import
        int count = 0;
        for (Element element : elements) {
            if (count % 1000 == 0) {
                LOGGER.debug("adding elements... " + count);
            }
            addElement(graph, element);
            count++;
        }
        LOGGER.debug("added " + count + " elements");
    }

    @Override
    public GraphQuery queryGraph(Graph graph, String queryString, Authorizations authorizations) {
        return new ElasticSearchNestedGraphQuery(client, indexName, graph, queryString, this.propertyDefinitions, authorizations);
    }

    @Override
    public VertexQuery queryVertex(Graph graph, Vertex vertex, String queryString, Authorizations authorizations) {
        return new DefaultVertexQuery(graph, vertex, queryString, this.propertyDefinitions, authorizations);
    }

    public TransportClient getClient() {
        return client;
    }

    public String getIndexName() {
        return indexName;
    }
}
