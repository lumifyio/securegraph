package org.securegraph.elasticsearch;

import org.securegraph.GraphConfiguration;
import org.securegraph.SecureGraphException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class ElasticSearchSearchIndexConfiguration {
    private static final Logger LOGGER = LoggerFactory.getLogger(ElasticSearchSearchIndexConfiguration.class);
    public static final String CONFIG_STORE_SOURCE_DATA = "storeSourceData";
    public static final boolean DEFAULT_STORE_SOURCE_DATA = false;
    public static final String CONFIG_ES_LOCATIONS = "locations";
    public static final String CONFIG_INDEX_NAME = "indexName";
    public static final String DEFAULT_INDEX_NAME = "securegraph";
    public static final String CONFIG_INDICES_TO_QUERY = "indicesToQuery";
    public static final String CONFIG_IN_EDGE_BOOST = "inEdgeBoost";
    public static final double DEFAULT_IN_EDGE_BOOST = 1.2;
    public static final String CONFIG_OUT_EDGE_BOOST = "outEdgeBoost";
    public static final double DEFAULT_OUT_EDGE_BOOST = 1.1;
    public static final String CONFIG_USE_EDGE_BOOST = "useEdgeBoost";
    public static final boolean DEFAULT_USE_EDGE_BOOST = true;
    public static final String CONFIG_UPDATE_EDGE_BOOST = "updateEdgeBoost";
    public static final boolean DEFAULT_UPDATE_EDGE_BOOST = true;
    public static final String CONFIG_INDEX_EDGES = "indexEdges";
    public static final boolean DEFAULT_INDEX_EDGES = true;
    public static final boolean DEFAULT_AUTO_FLUSH = false;
    public static final String CONFIG_CLUSTER_NAME = "clusterName";
    public static final String DEFAULT_CLUSTER_NAME = null;
    public static final String CONFIG_PORT = "port";
    public static final int DEFAULT_PORT = 9300;

    private final boolean autoFlush;
    private final boolean storeSourceData;
    private final String[] esLocations;
    private final double inEdgeBoost;
    private final double outEdgeBoost;
    private final boolean useEdgeBoost;
    private final boolean updateEdgeBoost;
    private final String defaultIndexName;
    private final String[] indicesToQuery;
    private final boolean indexEdges;
    private final String clusterName;
    private final int port;

    public ElasticSearchSearchIndexConfiguration(Map config) {
        esLocations = getElasticSearchLocations(config);
        defaultIndexName = getDefaultIndexName(config);
        indicesToQuery = getIndicesToQuery(config, defaultIndexName);
        inEdgeBoost = getInEdgeBoost(config);
        outEdgeBoost = getOutEdgeBoost(config);
        useEdgeBoost = getUseEdgeBoost(config);
        updateEdgeBoost = getUpdateEdgeBoost(config);
        indexEdges = getIndexEdges(config);
        storeSourceData = getStoreSourceData(config);
        autoFlush = getAutoFlush(config);
        clusterName = getClusterName(config);
        port = getPort(config);
    }

    public boolean isAutoFlush() {
        return autoFlush;
    }

    public boolean isStoreSourceData() {
        return storeSourceData;
    }

    public String[] getEsLocations() {
        return esLocations;
    }

    public double getInEdgeBoost() {
        return inEdgeBoost;
    }

    public double getOutEdgeBoost() {
        return outEdgeBoost;
    }

    public boolean isUseEdgeBoost() {
        return useEdgeBoost;
    }

    public boolean isUpdateEdgeBoost() {
        return isUseEdgeBoost() && updateEdgeBoost;
    }

    public String getDefaultIndexName() {
        return defaultIndexName;
    }

    public String[] getIndicesToQuery() {
        return indicesToQuery;
    }

    public boolean isIndexEdges() {
        return indexEdges;
    }

    public String getClusterName() {
        return clusterName;
    }

    public int getPort() {
        return port;
    }

    private static boolean getAutoFlush(Map config) {
        boolean autoFlush = getBoolean(config, GraphConfiguration.AUTO_FLUSH, DEFAULT_AUTO_FLUSH);
        LOGGER.info("Auto flush: " + autoFlush);
        return autoFlush;
    }

    private static boolean getStoreSourceData(Map config) {
        boolean storeSourceData = getBoolean(config, GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + CONFIG_STORE_SOURCE_DATA, DEFAULT_STORE_SOURCE_DATA);
        LOGGER.info("Store source data: " + storeSourceData);
        return storeSourceData;
    }

    private boolean getIndexEdges(Map config) {
        boolean indexEdges = getBoolean(config, GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + CONFIG_INDEX_EDGES, DEFAULT_INDEX_EDGES);
        LOGGER.info("index edges: " + indexEdges);
        return indexEdges;
    }

    private static boolean getUseEdgeBoost(Map config) {
        boolean useEdgeBoost = getBoolean(config, GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + CONFIG_USE_EDGE_BOOST, DEFAULT_USE_EDGE_BOOST);
        LOGGER.info("Use edge boost: " + useEdgeBoost);
        return useEdgeBoost;
    }

    private static boolean getUpdateEdgeBoost(Map config) {
        boolean updateEdgeBoost = getBoolean(config, GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + CONFIG_UPDATE_EDGE_BOOST, DEFAULT_UPDATE_EDGE_BOOST);
        LOGGER.info("Update edge boost: " + updateEdgeBoost);
        return updateEdgeBoost;
    }

    private static double getOutEdgeBoost(Map config) {
        double outEdgeBoost = getDouble(config, GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + CONFIG_OUT_EDGE_BOOST, DEFAULT_OUT_EDGE_BOOST);
        LOGGER.info("Out Edge Boost: " + outEdgeBoost);
        return outEdgeBoost;
    }

    private static double getInEdgeBoost(Map config) {
        double inEdgeBoost = getDouble(config, GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + CONFIG_IN_EDGE_BOOST, DEFAULT_IN_EDGE_BOOST);
        LOGGER.info("In Edge Boost: " + inEdgeBoost);
        return inEdgeBoost;
    }

    private static String[] getIndicesToQuery(Map config, String defaultIndexName) {
        String[] indicesToQuery;
        String indicesToQueryString = (String) config.get(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + CONFIG_INDICES_TO_QUERY);
        if (indicesToQueryString == null) {
            indicesToQuery = new String[]{defaultIndexName};
        } else {
            indicesToQuery = indicesToQueryString.split(",");
            for (int i = 0; i < indicesToQuery.length; i++) {
                indicesToQuery[i] = indicesToQuery[i].trim();
            }
        }
        if (LOGGER.isInfoEnabled()) {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < indicesToQuery.length; i++) {
                if (i > 0) {
                    sb.append(',');
                }
                sb.append(indicesToQuery[i]);
            }
            LOGGER.info("Indices to query: " + sb.toString());
        }
        return indicesToQuery;
    }

    private static String[] getElasticSearchLocations(Map config) {
        String esLocationsString = (String) config.get(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + CONFIG_ES_LOCATIONS);
        if (esLocationsString == null) {
            throw new SecureGraphException(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + CONFIG_ES_LOCATIONS + " is a required configuration parameter");
        }
        LOGGER.info("Using elastic search locations: " + esLocationsString);
        return esLocationsString.split(",");
    }

    private static String getDefaultIndexName(Map config) {
        String defaultIndexName = getString(config, GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + CONFIG_INDEX_NAME, DEFAULT_INDEX_NAME);
        LOGGER.info("Default index name: " + defaultIndexName);
        return defaultIndexName;
    }

    private static String getClusterName(Map config) {
        String clusterName = getString(config, GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + CONFIG_CLUSTER_NAME, DEFAULT_CLUSTER_NAME);
        LOGGER.info("Cluster name: " + clusterName);
        return clusterName;
    }

    private static int getPort(Map config) {
        int port = getInt(config, GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + CONFIG_PORT, DEFAULT_PORT);
        LOGGER.info("Port: " + port);
        return port;
    }

    private static boolean getBoolean(Map config, String configKey, boolean defaultValue) {
        Object obj = config.get(configKey);
        if (obj == null) {
            return defaultValue;
        }
        if (obj instanceof String) {
            return Boolean.parseBoolean((String) obj);
        }
        if (obj instanceof Boolean) {
            return (boolean) obj;
        }
        throw new SecureGraphException("Could not parse config option to boolean: " + configKey + " found " + obj);
    }

    private static double getDouble(Map config, String configKey, double defaultValue) {
        Object obj = config.get(configKey);
        if (obj == null) {
            return defaultValue;
        }
        if (obj instanceof String) {
            return Double.parseDouble((String) obj);
        }
        if (obj instanceof Double) {
            return (double) obj;
        }
        throw new SecureGraphException("Could not parse config option to double: " + configKey + " found " + obj);
    }

    private static int getInt(Map config, String configKey, int defaultValue) {
        Object obj = config.get(configKey);
        if (obj == null) {
            return defaultValue;
        }
        if (obj instanceof String) {
            return Integer.parseInt((String) obj);
        }
        if (obj instanceof Integer) {
            return (int) obj;
        }
        throw new SecureGraphException("Could not parse config option to int: " + configKey + " found " + obj);
    }

    private static String getString(Map config, String configKey, String defaultValue) {
        String str = (String) config.get(configKey);
        if (str == null) {
            return defaultValue;
        }
        return str;
    }
}
