package com.altamiracorp.securegraph.elasticsearch.helpers;

import com.altamiracorp.securegraph.Graph;
import com.altamiracorp.securegraph.GraphConfiguration;
import com.altamiracorp.securegraph.elasticsearch.ElasticSearchSearchIndex;
import com.altamiracorp.securegraph.inmemory.InMemoryGraph;

import java.util.HashMap;
import java.util.Map;

public class TestHelpers {
    public static Graph createGraph() {
        Map config = new HashMap();
        config.put(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX, ElasticSearchSearchIndex.class.getName());
        GraphConfiguration configuration = new GraphConfiguration(config);
        return new InMemoryGraph(configuration, configuration.createIdGenerator(), configuration.createSearchIndex());
    }

    public static void before() {
    }

    public static void after() {
    }
}
