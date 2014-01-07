package com.altamiracorp.securegraph.elasticsearch;

import com.altamiracorp.securegraph.*;
import com.altamiracorp.securegraph.query.DefaultVertexQuery;
import com.altamiracorp.securegraph.query.GraphQuery;
import com.altamiracorp.securegraph.query.VertexQuery;
import com.altamiracorp.securegraph.search.SearchIndex;
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
    private String indexName;
    private Map<String, Boolean> existingProperties = new HashMap<String, Boolean>();

    public ElasticSearchSearchIndex(Map config) {
        String esLocationsString = (String) config.get(ES_LOCATIONS);
        if (esLocationsString == null) {
            throw new SecureGraphException(ES_LOCATIONS + " is a required configuration parameter");
        }
        String[] esLocations = esLocationsString.split(",");

        indexName = (String) config.get(INDEX_NAME);
        if (indexName == null) {
            indexName = DEFAULT_INDEX_NAME;
        }

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
                        .startObject("properties");
                mapping.startObject(ELEMENT_TYPE_FIELD_NAME).field("type", "string").endObject();
                mapping
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
                jsonBuilder.field(property.getName(), property.getValue());
            }

            IndexResponse response = client
                    .prepareIndex(indexName, ELEMENT_TYPE, element.getId().toString())
                    .setSource(jsonBuilder.endObject())
                    .execute()
                    .actionGet();
            if (response.getId() == null) {
                throw new SecureGraphException("Could not index document " + element.getId());
            }
            LOGGER.debug(response.toString());

            // TODO get autoflush setting
            client.admin().indices().prepareFlush(indexName).execute().actionGet();
        } catch (Exception e) {
            throw new SecureGraphException("Could not add document", e);
        }
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

        Class dataType = property.getValue().getClass();

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
        } else if (dataType == Long.class) {
            LOGGER.debug("Registering long type for {}", propertyName);
            mapping.field("type", "long");
        } else if (dataType == Boolean.class) {
            LOGGER.debug("Registering boolean type for {}", propertyName);
            mapping.field("type", "boolean");
        } else {
            throw new SecureGraphException("Unexpected value type: " + dataType.getName());
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
