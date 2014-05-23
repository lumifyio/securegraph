package org.securegraph.elasticsearch.helpers;

import org.apache.commons.io.FileUtils;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.securegraph.Graph;
import org.securegraph.GraphConfiguration;
import org.securegraph.elasticsearch.ElasticSearchParentChildSearchIndex;
import org.securegraph.elasticsearch.ElasticSearchSearchIndexBase;
import org.securegraph.inmemory.InMemoryGraph;
import org.securegraph.inmemory.InMemoryGraphConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class TestHelpers {
    private static final Logger LOGGER = LoggerFactory.getLogger(TestHelpers.class);
    private static File tempDir;
    private static Node elasticSearchNode;
    private static String addr;
    private static String clusterName;

    public static Graph createGraph() {
        Map config = new HashMap();
        config.put(GraphConfiguration.AUTO_FLUSH, true);
        config.put(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX, ElasticSearchParentChildSearchIndex.class.getName());
        config.put(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + ElasticSearchSearchIndexBase.CONFIG_ES_LOCATIONS, addr);
        config.put(ElasticSearchSearchIndexBase.SETTING_CLUSTER_NAME, clusterName);
        InMemoryGraphConfiguration configuration = new InMemoryGraphConfiguration(config);
        return new InMemoryGraph(configuration, configuration.createIdGenerator(), configuration.createSearchIndex());
    }

    public static void before() throws IOException {
        tempDir = File.createTempFile("elasticsearch-temp", Long.toString(System.nanoTime()));
        tempDir.delete();
        tempDir.mkdir();
        LOGGER.info("writing to: " + tempDir);

        clusterName = UUID.randomUUID().toString();
        elasticSearchNode = NodeBuilder
                .nodeBuilder()
                .local(false)
                .clusterName(clusterName)
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
