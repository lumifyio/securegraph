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

    private final boolean useEdgeBoost;
    private final boolean updateEdgeBoost;

    public EdgeCountScoringStrategyConfiguration(GraphConfiguration config) {
        useEdgeBoost = getUseEdgeBoost(config);
        updateEdgeBoost = getUpdateEdgeBoost(config);
    }

    public boolean isUseEdgeBoost() {
        return useEdgeBoost;
    }

    public boolean isUpdateEdgeBoost() {
        return isUseEdgeBoost() && updateEdgeBoost;
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
}
