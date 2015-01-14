package org.securegraph.elasticsearch;

import org.securegraph.GraphConfiguration;
import org.securegraph.SecureGraphException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class ElasticSearchSearchIndexConfiguration {
    private static final Logger LOGGER = LoggerFactory.getLogger(ElasticSearchSearchIndexConfiguration.class);
    public static final String CONFIG_STORE_SOURCE_DATA = "storeSourceData";
    public static final String CONFIG_ES_LOCATIONS = "locations";
    public static final String CONFIG_INDEX_NAME = "indexName";
    private static final String DEFAULT_INDEX_NAME = "securegraph";
    public static final String CONFIG_INDICES_TO_QUERY = "indicesToQuery";
    public static final String CONFIG_IN_EDGE_BOOST = "inEdgeBoost";
    private static final double DEFAULT_IN_EDGE_BOOST = 1.2;
    public static final String CONFIG_OUT_EDGE_BOOST = "outEdgeBoost";
    private static final double DEFAULT_OUT_EDGE_BOOST = 1.1;
    public static final String CONFIG_USE_EDGE_BOOST = "useEdgeBoost";
    private static final boolean DEFAULT_USE_EDGE_BOOST = true;
    public static final String CONFIG_INDEX_EDGES = "indexEdges";
    private static final boolean DEFAULT_INDEX_EDGES = true;

    private final boolean autoFlush;
    private final boolean storeSourceData;
    private final String[] esLocations;
    private final double inEdgeBoost;
    private final double outEdgeBoost;
    private final boolean useEdgeBoost;
    private final String defaultIndexName;
    private final String[] indicesToQuery;
    private final boolean indexEdges;

    public ElasticSearchSearchIndexConfiguration(Map config) {
        esLocations = getElasticSearchLocations(config);
        defaultIndexName = getDefaultIndexName(config);
        indicesToQuery = getIndicesToQuery(config, defaultIndexName);
        inEdgeBoost = getInEdgeBoost(config);
        outEdgeBoost = getOutEdgeBoost(config);
        useEdgeBoost = getUseEdgeBoost(config);
        indexEdges = getIndexEdges(config);
        storeSourceData = getStoreSourceData(config);
        autoFlush = getAutoFlush(config);
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

    public String getDefaultIndexName() {
        return defaultIndexName;
    }

    public String[] getIndicesToQuery() {
        return indicesToQuery;
    }

    public boolean isIndexEdges() {
        return indexEdges;
    }

    private static boolean getAutoFlush(Map config) {
        boolean autoFlush;
        // TODO convert this to use a proper config object
        Object autoFlushObj = config.get(GraphConfiguration.AUTO_FLUSH);
        autoFlush = autoFlushObj != null && "true".equals(autoFlushObj.toString());
        LOGGER.info("Auto flush: " + autoFlush);
        return autoFlush;
    }

    private static boolean getStoreSourceData(Map config) {
        boolean storeSourceData;
        Object storeSourceDataConfig = config.get(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + CONFIG_STORE_SOURCE_DATA);
        storeSourceData = storeSourceDataConfig != null && "true".equals(storeSourceDataConfig.toString());
        LOGGER.info("Store source data: " + storeSourceData);
        return storeSourceData;
    }

    private boolean getIndexEdges(Map config) {
        boolean indexEdges;

        String indexEdgesString = (String) config.get(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + CONFIG_INDEX_EDGES);
        if (indexEdgesString == null) {
            indexEdges = DEFAULT_INDEX_EDGES;
        } else {
            indexEdges = Boolean.parseBoolean(indexEdgesString);
        }
        LOGGER.info("index edges: " + indexEdges);
        return indexEdges;
    }

    private static boolean getUseEdgeBoost(Map config) {
        boolean useEdgeBoost;

        String useEdgeBoostString = (String) config.get(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + CONFIG_USE_EDGE_BOOST);
        if (useEdgeBoostString == null) {
            useEdgeBoost = DEFAULT_USE_EDGE_BOOST;
        } else {
            useEdgeBoost = Boolean.parseBoolean(useEdgeBoostString);
        }
        LOGGER.info("Use edge boost: " + useEdgeBoost);
        return useEdgeBoost;
    }

    private static double getOutEdgeBoost(Map config) {
        double outEdgeBoost;

        String outEdgeBoostString = (String) config.get(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + CONFIG_OUT_EDGE_BOOST);
        if (outEdgeBoostString == null) {
            outEdgeBoost = DEFAULT_OUT_EDGE_BOOST;
        } else {
            outEdgeBoost = Double.parseDouble(outEdgeBoostString);
        }
        LOGGER.info("Out Edge Boost: " + outEdgeBoost);
        return outEdgeBoost;
    }

    private static double getInEdgeBoost(Map config) {
        double inEdgeBoost;

        String inEdgeBoostString = (String) config.get(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + CONFIG_IN_EDGE_BOOST);
        if (inEdgeBoostString == null) {
            inEdgeBoost = DEFAULT_IN_EDGE_BOOST;
        } else {
            inEdgeBoost = Double.parseDouble(inEdgeBoostString);
        }
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
        String defaultIndexName = (String) config.get(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + CONFIG_INDEX_NAME);
        if (defaultIndexName == null) {
            defaultIndexName = DEFAULT_INDEX_NAME;
        }
        LOGGER.info("Default index name: " + defaultIndexName);
        return defaultIndexName;
    }
}
