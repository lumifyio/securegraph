package org.securegraph.elasticsearch.helpers;

import org.securegraph.Graph;
import org.securegraph.GraphConfiguration;
import org.securegraph.elasticsearch.ElasticSearchSearchIndex;
import org.securegraph.inmemory.InMemoryGraph;
import org.securegraph.inmemory.InMemoryGraphConfiguration;
import org.apache.commons.io.FileUtils;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class TestHelpers {
    private static final Logger LOGGER = LoggerFactory.getLogger(TestHelpers.class);
    private static File tempDir;
    private static Node elasticSearchNode;
    private static String addr;

    public static Graph createGraph() {
        Map config = new HashMap();
        config.put(GraphConfiguration.AUTO_FLUSH, true);
        config.put(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX, ElasticSearchSearchIndex.class.getName());
        config.put(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + ElasticSearchSearchIndex.ES_LOCATIONS, addr);
        InMemoryGraphConfiguration configuration = new InMemoryGraphConfiguration(config);
        return new InMemoryGraph(configuration, configuration.createIdGenerator(), configuration.createSearchIndex());
    }

    public static void before() throws IOException {
        tempDir = File.createTempFile("elasticsearch-temp", Long.toString(System.nanoTime()));
        tempDir.delete();
        tempDir.mkdir();
        LOGGER.info("writing to: " + tempDir);

        elasticSearchNode = NodeBuilder
                .nodeBuilder()
                .local(false)
                .settings(
                        ImmutableSettings.settingsBuilder()
                                .put("gateway.type", "local")
                                .put("path.data", new File(tempDir, "data").getAbsolutePath())
                                .put("path.logs", new File(tempDir, "logs").getAbsolutePath())
                                .put("path.work", new File(tempDir, "work").getAbsolutePath())
                ).node();
        elasticSearchNode.start();

        ClusterStateResponse response = elasticSearchNode.client().admin().cluster().prepareState().execute().actionGet();
        addr = response.getState().getNodes().getNodes().values().iterator().next().value.getAddress().toString();
        addr = addr.substring("inet[/".length());
        addr = addr.substring(0, addr.length() - 1);
    }

    public static void after() throws IOException {
        if (elasticSearchNode != null) {
            elasticSearchNode.stop();
            elasticSearchNode.close();
        }
        FileUtils.deleteDirectory(tempDir);
    }
}
