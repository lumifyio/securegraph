package org.securegraph.elasticsearch;

import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.status.IndicesStatusResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.hppc.cursors.ObjectObjectCursor;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

public abstract class ElasticSearchSearchIndexBase implements SearchIndex {
    private static final Logger LOGGER = LoggerFactory.getLogger(ElasticSearchSearchIndexBase.class);
    public static final String STORE_SOURCE_DATA = "storeSourceData";
    public static final String CONFIG_ES_LOCATIONS = "locations";
    public static final String CONFIG_INDEX_NAME = "indexName";
    private static final String DEFAULT_INDEX_NAME = "securegraph";
    public static final String CONFIG_IN_EDGE_BOOST = "inEdgeBoost";
    private static final double DEFAULT_IN_EDGE_BOOST = 1.2;
    public static final String CONFIG_OUT_EDGE_BOOST = "outEdgeBoost";
    private static final double DEFAULT_OUT_EDGE_BOOST = 1.1;
    public static final String CONFIG_USE_EDGE_BOOST = "useEdgeBoost";
    private static final boolean DEFAULT_USE_EDGE_BOOST = true;
    public static final String ELEMENT_TYPE = "element";
    public static final String ELEMENT_TYPE_FIELD_NAME = "__elementType";
    public static final String VISIBILITY_FIELD_NAME = "__visibility";
    public static final String IN_EDGE_COUNT_FIELD_NAME = "__inEdgeCount";
    public static final String OUT_EDGE_COUNT_FIELD_NAME = "__outEdgeCount";
    public static final String ELEMENT_TYPE_VERTEX = "vertex";
    public static final String ELEMENT_TYPE_EDGE = "edge";
    public static final String SETTING_CLUSTER_NAME = "clusterName";
    public static final int DEFAULT_ES_PORT = 9300;
    public static final String EXACT_MATCH_PROPERTY_NAME_SUFFIX = "_exactMatch";
    private final TransportClient client;
    private final boolean autoflush;
    private String indexName;
    private Map<String, PropertyDefinition> propertyDefinitions = new HashMap<String, PropertyDefinition>();
    private String[] esLocations;
    private double inEdgeBoost;
    private double outEdgeBoost;
    private boolean useEdgeBoost;

    protected ElasticSearchSearchIndexBase(Map config) {
        readConfig(config);

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

        ensureIndexCreated(storeSourceData);
        loadPropertyDefinitions();
    }

    protected void readConfig(Map config) {
        // Locations
        String esLocationsString = (String) config.get(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + CONFIG_ES_LOCATIONS);
        if (esLocationsString == null) {
            throw new SecureGraphException(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + CONFIG_ES_LOCATIONS + " is a required configuration parameter");
        }
        LOGGER.info("Using elastic search locations: " + esLocationsString);
        esLocations = esLocationsString.split(",");

        // Index Name
        indexName = (String) config.get(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + CONFIG_INDEX_NAME);
        if (indexName == null) {
            indexName = DEFAULT_INDEX_NAME;
        }
        LOGGER.info("Using index: " + indexName);

        // In-Edge Boost
        String inEdgeBoostString = (String) config.get(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + CONFIG_IN_EDGE_BOOST);
        if (inEdgeBoostString == null) {
            inEdgeBoost = DEFAULT_IN_EDGE_BOOST;
        } else {
            inEdgeBoost = Double.parseDouble(inEdgeBoostString);
        }
        LOGGER.info("In Edge Boost: " + inEdgeBoost);

        // Out-Edge Boost
        String outEdgeBoostString = (String) config.get(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + CONFIG_OUT_EDGE_BOOST);
        if (outEdgeBoostString == null) {
            outEdgeBoost = DEFAULT_OUT_EDGE_BOOST;
        } else {
            outEdgeBoost = Double.parseDouble(outEdgeBoostString);
        }
        LOGGER.info("Out Edge Boost: " + outEdgeBoost);

        // Use Edge Boost
        String useEdgeBoostString = (String) config.get(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + CONFIG_USE_EDGE_BOOST);
        if (useEdgeBoostString == null) {
            useEdgeBoost = DEFAULT_USE_EDGE_BOOST;
        } else {
            useEdgeBoost = Boolean.parseBoolean(useEdgeBoostString);
        }
        LOGGER.info("Use edge boost: " + useEdgeBoost);
    }

    protected void ensureIndexCreated(boolean storeSourceData) {
        if (!client.admin().indices().prepareExists(indexName).execute().actionGet().isExists()) {
            try {
                createIndex(storeSourceData);

                IndicesStatusResponse statusResponse = client.admin().indices().prepareStatus(indexName).execute().actionGet();
                LOGGER.debug(statusResponse.toString());
            } catch (IOException e) {
                throw new SecureGraphException("Could not create index", e);
            }
        }
    }

    protected void createIndex(boolean storeSourceData) throws IOException {
        XContentBuilder mapping = XContentFactory.jsonBuilder()
                .startObject()
                .startObject(ELEMENT_TYPE)
                .startObject("_source").field("enabled", storeSourceData).endObject()
                .startObject("properties")
                .startObject(ELEMENT_TYPE_FIELD_NAME).field("type", "string").field("store", "true").endObject()
                .startObject(IN_EDGE_COUNT_FIELD_NAME).field("type", "integer").field("store", "true").endObject()
                .startObject(OUT_EDGE_COUNT_FIELD_NAME).field("type", "integer").field("store", "true").endObject()
                .endObject()
                .endObject()
                .endObject();
        CreateIndexResponse createResponse = client.admin().indices().prepareCreate(indexName).addMapping(ELEMENT_TYPE, mapping).execute().actionGet();
        LOGGER.debug(createResponse.toString());
    }

    private void loadPropertyDefinitions() {
        Map<String, String> propertyTypes = getPropertyTypesFromServer();
        for (Map.Entry<String, String> property : propertyTypes.entrySet()) {
            String propertyName = property.getKey();
            Class dataType = elasticSearchTypeToClass(property.getValue());
            Set<TextIndexHint> indexHints = new HashSet<TextIndexHint>();

            if (dataType == String.class) {
                if (propertyName.endsWith(EXACT_MATCH_PROPERTY_NAME_SUFFIX)) {
                    indexHints.add(TextIndexHint.EXACT_MATCH);
                    if (propertyTypes.containsKey(propertyName.substring(0, propertyName.length() - EXACT_MATCH_PROPERTY_NAME_SUFFIX.length()))) {
                        indexHints.add(TextIndexHint.FULL_TEXT);
                    }
                } else {
                    indexHints.add(TextIndexHint.FULL_TEXT);
                    if (propertyTypes.containsKey(propertyName + EXACT_MATCH_PROPERTY_NAME_SUFFIX)) {
                        indexHints.add(TextIndexHint.EXACT_MATCH);
                    }
                }
            }

            PropertyDefinition propertyDefinition = new PropertyDefinition(propertyName, dataType, indexHints);
            this.propertyDefinitions.put(propertyName, propertyDefinition);
        }
    }

    private Class elasticSearchTypeToClass(String typeName) {
        if ("string".equals(typeName)) {
            return String.class;
        }
        if ("float".equals(typeName)) {
            return Float.class;
        }
        if ("double".equals(typeName)) {
            return Double.class;
        }
        if ("byte".equals(typeName)) {
            return Byte.class;
        }
        if ("short".equals(typeName)) {
            return Short.class;
        }
        if ("integer".equals(typeName)) {
            return Integer.class;
        }
        if ("date".equals(typeName)) {
            return Date.class;
        }
        if ("long".equals(typeName)) {
            return Long.class;
        }
        if ("boolean".equals(typeName)) {
            return Boolean.class;
        }
        if ("geo_point".equals(typeName)) {
            return GeoPoint.class;
        }
        throw new SecureGraphException("Unhandled type: " + typeName);
    }

    private Map<String, String> getPropertyTypesFromServer() {
        Map<String, String> propertyTypes = new HashMap<String, String>();
        try {
            ClusterState cs = client.admin().cluster().prepareState().setIndices(indexName).execute().actionGet().getState();
            IndexMetaData imd = cs.getMetaData().index(indexName);
            for (ObjectObjectCursor<String, MappingMetaData> m : imd.getMappings()) {
                Map<String, Object> sourceAsMap = m.value.getSourceAsMap();
                Map properties = (Map) sourceAsMap.get("properties");
                for (Object propertyObj : properties.entrySet()) {
                    Map.Entry property = (Map.Entry) propertyObj;
                    String propertyName = (String) property.getKey();
                    Map propertyAttributes = (Map) property.getValue();
                    String propertyType = (String) propertyAttributes.get("type");
                    propertyTypes.put(propertyName, propertyType);
                }
            }
        } catch (IOException ex) {
            throw new SecureGraphException("Could not get current properties from elastic search", ex);
        }
        return propertyTypes;
    }

    @Override
    public abstract void addElement(Graph graph, Element element, Authorizations authorizations);

    @Override
    public void removeElement(Graph graph, Element element) {
        // TODO write me
    }

    @Override
    public void addElements(Graph graph, Iterable<Element> elements, Authorizations authorizations) {
        // TODO change this to use elastic search bulk import
        int count = 0;
        for (Element element : elements) {
            if (count % 1000 == 0) {
                LOGGER.debug("adding elements... " + count);
            }
            addElement(graph, element, authorizations);
            count++;
        }
        LOGGER.debug("added " + count + " elements");
    }

    @Override
    public abstract GraphQuery queryGraph(Graph graph, String queryString, Authorizations authorizations);

    @Override
    public VertexQuery queryVertex(Graph graph, Vertex vertex, String queryString, Authorizations authorizations) {
        return new DefaultVertexQuery(graph, vertex, queryString, this.propertyDefinitions, authorizations);
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
    public void addPropertyDefinition(PropertyDefinition propertyDefinition) throws IOException {
        if (propertyDefinition.getDataType() == String.class) {
            if (propertyDefinition.getTextIndexHints().contains(TextIndexHint.EXACT_MATCH)) {
                addPropertyToIndex(propertyDefinition.getPropertyName() + EXACT_MATCH_PROPERTY_NAME_SUFFIX, String.class, false, propertyDefinition.getBoost());
            }
            if (propertyDefinition.getTextIndexHints().contains(TextIndexHint.FULL_TEXT)) {
                addPropertyToIndex(propertyDefinition.getPropertyName(), String.class, true, propertyDefinition.getBoost());
            }
        } else {
            addPropertyToIndex(propertyDefinition);
        }
        this.propertyDefinitions.put(propertyDefinition.getPropertyName(), propertyDefinition);
    }

    @Override
    public boolean isFieldBoostSupported() {
        return true;
    }

    @Override
    public boolean isEdgeBoostSupported() {
        return true;
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

    private void addPropertyToIndex(String propertyName, Class dataType, boolean analyzed) throws IOException {
        addPropertyToIndex(propertyName, dataType, analyzed, null);
    }

    private void addPropertyToIndex(PropertyDefinition propertyDefinition) throws IOException {
        addPropertyToIndex(propertyDefinition.getPropertyName(), propertyDefinition.getDataType(), true, propertyDefinition.getBoost());
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
            addPropertyToIndex(propertyName, dataType, true);
        } else if (propertyValue instanceof String) {
            dataType = String.class;
            addPropertyToIndex(propertyName + EXACT_MATCH_PROPERTY_NAME_SUFFIX, dataType, false);
            addPropertyToIndex(propertyName, dataType, true);
        } else {
            dataType = propertyValue.getClass();
            addPropertyToIndex(propertyName, dataType, true);
        }
    }

    protected abstract void addPropertyToIndex(String propertyName, Class dataType, boolean analyzed, Double boost) throws IOException;

    protected boolean shouldIgnoreType(Class dataType) {
        if (dataType == byte[].class) {
            return true;
        }
        return false;
    }

    public TransportClient getClient() {
        return client;
    }

    public String getIndexName() {
        return indexName;
    }

    public boolean isAutoflush() {
        return autoflush;
    }

    public boolean isUseEdgeBoost() {
        return useEdgeBoost;
    }

    protected Map<String, PropertyDefinition> getPropertyDefinitions() {
        return propertyDefinitions;
    }

    public double getInEdgeBoost() {
        return inEdgeBoost;
    }

    public double getOutEdgeBoost() {
        return outEdgeBoost;
    }

    protected void addTypeToMapping(XContentBuilder mapping, String propertyName, Class dataType, boolean analyzed, Double boost) throws IOException {
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
            LOGGER.debug("Registering geo_point type for {}", propertyName);
            mapping.field("type", "geo_point");
        } else if (Number.class.isAssignableFrom(dataType)) {
            LOGGER.debug("Registering double type for {}", propertyName);
            mapping.field("type", "double");
        } else {
            throw new SecureGraphException("Unexpected value type for property \"" + propertyName + "\": " + dataType.getName());
        }

        if (boost != null) {
            mapping.field("boost", boost.doubleValue());
        }
    }
}
