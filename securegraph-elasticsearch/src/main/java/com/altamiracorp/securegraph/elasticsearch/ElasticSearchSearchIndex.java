package com.altamiracorp.securegraph.elasticsearch;

import com.altamiracorp.securegraph.*;
import com.altamiracorp.securegraph.property.StreamingPropertyValue;
import com.altamiracorp.securegraph.query.DefaultVertexQuery;
import com.altamiracorp.securegraph.query.GraphQuery;
import com.altamiracorp.securegraph.query.VertexQuery;
import com.altamiracorp.securegraph.search.SearchIndex;
import com.altamiracorp.securegraph.type.GeoPoint;
import com.altamiracorp.securegraph.util.StreamUtils;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.admin.indices.status.IndicesStatusResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class ElasticSearchSearchIndex implements SearchIndex {
    private static final Logger LOGGER = LoggerFactory.getLogger(ElasticSearchSearchIndex.class);
    public static final String ES_LOCATIONS = "locations";
    public static final String INDEX_NAME = "indexName";
    private static final String DEFAULT_INDEX_NAME = "securegraph";
    public static final String ELEMENT_TYPE = "element";
    public static final String ELEMENT_TYPE_FIELD_NAME = "__elementType";
    public static final String ELEMENT_TYPE_VERTEX = "vertex";
    public static final String ELEMENT_TYPE_EDGE = "edge";
    public static final int DEFAULT_ES_PORT = 9300;
    private final TransportClient client;
    private final boolean autoflush;
    private String indexName;
    private Map<String, Boolean> existingProperties = new HashMap<String, Boolean>();

    public ElasticSearchSearchIndex(Map config) {
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

        // TODO convert this to use a proper config object
        autoflush = "true".equals(config.get(GraphConfiguration.AUTO_FLUSH));
        LOGGER.info("Auto flush: " + autoflush);

        client = new TransportClient();
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
                        .field("enabled", false)
                        .endObject()
                        .startObject("properties")
                        .startObject(ELEMENT_TYPE_FIELD_NAME)
                        .field("type", "string")
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
            XContentBuilder jsonBuilder = XContentFactory.jsonBuilder()
                    .startObject();

            if (element instanceof Vertex) {
                jsonBuilder.field(ELEMENT_TYPE_FIELD_NAME, ELEMENT_TYPE_VERTEX);
            } else if (element instanceof Edge) {
                jsonBuilder.field(ELEMENT_TYPE_FIELD_NAME, ELEMENT_TYPE_EDGE);
            } else {
                throw new SecureGraphException("Unexpected element type " + element.getClass().getName());
            }

            for (Property property : element.getProperties()) {
                Object propertyValue = property.getValue();
                if (propertyValue instanceof GeoPoint) {
                    GeoPoint geoPoint = (GeoPoint) propertyValue;
                    Map<String, Object> propertyValueMap = new HashMap<String, Object>();
                    propertyValueMap.put("lat", geoPoint.getLatitude());
                    propertyValueMap.put("lon", geoPoint.getLongitude());
                    propertyValue = propertyValueMap;
                } else if (propertyValue instanceof StreamingPropertyValue) {
                    StreamingPropertyValue streamingPropertyValue = (StreamingPropertyValue) propertyValue;
                    if (!streamingPropertyValue.isSearchIndex()) {
                        continue;
                    }
                    Class valueType = streamingPropertyValue.getValueType();
                    if (valueType == String.class) {
                        propertyValue = StreamUtils.toString(streamingPropertyValue.getInputStream());
                    } else {
                        throw new SecureGraphException("Unhandled StreamingPropertyValue type: " + valueType.getName());
                    }
                }
                jsonBuilder.field(property.getName(), propertyValue);
            }

            IndexResponse response = client
                    .prepareIndex(indexName, ELEMENT_TYPE, element.getId().toString())
                    .setSource(jsonBuilder.endObject())
                    .execute()
                    .actionGet();
            if (response.getId() == null) {
                throw new SecureGraphException("Could not index document " + element.getId());
            }

            if (autoflush) {
                client.admin().indices().prepareFlush(indexName).execute().actionGet();
            }
        } catch (Exception e) {
            throw new SecureGraphException("Could not add document", e);
        }
    }

    @Override
    public void flush() {
        client.admin().indices().prepareFlush(indexName).execute().actionGet();
    }

    private void addPropertiesToIndex(Iterable<Property> properties) {
        try {
            for (Property property : properties) {
                addPropertyToIndex(property);
            }
        } catch (IOException e) {
            throw new SecureGraphException("Could not add properties to index", e);
        }
    }

    private void addPropertyToIndex(Property property) throws IOException {
        String propertyName = property.getName();

        if (existingProperties.get(propertyName) != null) {
            return;
        }

        XContentBuilder mapping = XContentFactory.jsonBuilder()
                .startObject()
                .startObject(ELEMENT_TYPE)
                .startObject("properties")
                .startObject(propertyName);

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

        if (dataType == String.class) {
            LOGGER.debug("Registering string type for {}", propertyName);
            mapping.field("type", "string");
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
        } else if (dataType == Date.class) {
            LOGGER.debug("Registering date type for {}", propertyName);
            mapping.field("type", "date");
        } else if (dataType == Long.class) {
            LOGGER.debug("Registering long type for {}", propertyName);
            mapping.field("type", "long");
        } else if (dataType == Boolean.class) {
            LOGGER.debug("Registering boolean type for {}", propertyName);
            mapping.field("type", "boolean");
        } else if (dataType == GeoPoint.class) {
            LOGGER.debug("Registering geo_point type for {}", propertyName);
            mapping.field("type", "geo_point");
        } else {
            throw new SecureGraphException("Unexpected value type for property \"" + propertyName + "\": " + dataType.getName());
        }

        mapping.field("store", "no");

        mapping
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

        existingProperties.put(propertyName, true);
    }

    @Override
    public void removeElement(Graph graph, Element element) {

    }

    @Override
    public GraphQuery queryGraph(Graph graph, String queryString, Authorizations authorizations) {
        return new ElasticSearchGraphQuery(client, indexName, graph, queryString, authorizations);
    }

    @Override
    public VertexQuery queryVertex(Graph graph, Vertex vertex, String queryString, Authorizations authorizations) {
        return new DefaultVertexQuery(graph, vertex, queryString, authorizations);
    }
}
