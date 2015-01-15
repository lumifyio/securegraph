package org.securegraph.elasticsearch.score;

import org.securegraph.GraphConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EdgeCountScoringStrategyConfiguration {
    private static final Logger LOGGER = LoggerFactory.getLogger(EdgeCountScoringStrategyConfiguration.class);
    public static final String CONFIG_USE_EDGE_BOOST = "useEdgeBoost";
    public static final boolean DEFAULT_USE_EDGE_BOOST = true;
    public static final String CONFIG_UPDATE_EDGE_BOOST = "updateEdgeBoost";
    public static final boolean DEFAULT_UPDATE_EDGE_BOOST = true;
    public static final String CONFIG_IN_EDGE_BOOST = "inEdgeBoost";
    public static final double DEFAULT_IN_EDGE_BOOST = 1.2;
    public static final String CONFIG_OUT_EDGE_BOOST = "outEdgeBoost";
    public static final double DEFAULT_OUT_EDGE_BOOST = 1.1;

    private final boolean useEdgeBoost;
    private final boolean updateEdgeBoost;
    private final double inEdgeBoost;
    private final double outEdgeBoost;

    public EdgeCountScoringStrategyConfiguration(GraphConfiguration config) {
        useEdgeBoost = getUseEdgeBoost(config);
        updateEdgeBoost = getUpdateEdgeBoost(config);
        inEdgeBoost = getInEdgeBoost(config);
        outEdgeBoost = getOutEdgeBoost(config);
    }

    public boolean isUseEdgeBoost() {
        return useEdgeBoost;
    }

    public boolean isUpdateEdgeBoost() {
        return isUseEdgeBoost() && updateEdgeBoost;
    }

    public double getInEdgeBoost() {
        return inEdgeBoost;
    }

    public double getOutEdgeBoost() {
        return outEdgeBoost;
    }

    private static boolean getUseEdgeBoost(GraphConfiguration config) {
        boolean useEdgeBoost = config.getBoolean(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + CONFIG_USE_EDGE_BOOST, DEFAULT_USE_EDGE_BOOST);
        LOGGER.info("Use edge boost: " + useEdgeBoost);
        return useEdgeBoost;
    }

    private static boolean getUpdateEdgeBoost(GraphConfiguration config) {
        boolean updateEdgeBoost = config.getBoolean(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + CONFIG_UPDATE_EDGE_BOOST, DEFAULT_UPDATE_EDGE_BOOST);
        LOGGER.info("Update edge boost: " + updateEdgeBoost);
        return updateEdgeBoost;
    }

    private static double getOutEdgeBoost(GraphConfiguration config) {
        double outEdgeBoost = config.getDouble(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + CONFIG_OUT_EDGE_BOOST, DEFAULT_OUT_EDGE_BOOST);
        LOGGER.info("Out Edge Boost: " + outEdgeBoost);
        return outEdgeBoost;
    }

    private static double getInEdgeBoost(GraphConfiguration config) {
        double inEdgeBoost = config.getDouble(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + CONFIG_IN_EDGE_BOOST, DEFAULT_IN_EDGE_BOOST);
        LOGGER.info("In Edge Boost: " + inEdgeBoost);
        return inEdgeBoost;
    }
}
