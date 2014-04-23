package org.securegraph.accumulo.helpers;

import org.securegraph.accumulo.AccumuloGraphConfiguration;
import org.securegraph.test.GraphTestBase;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class TestAccumuloCluster {
    private static final Logger LOGGER = LoggerFactory.getLogger(TestAccumuloCluster.class);
    private static final String ACCUMULO_USERNAME = "root";
    private static final String ACCUMULO_PASSWORD = "test";
    private static File tempDir;
    private static AccumuloGraphConfiguration config;
    private static MiniAccumuloCluster accumulo;

    public static AccumuloGraphConfiguration getConfig() {
        return config;
    }

    public static void start() throws IOException, InterruptedException {
        if (accumulo != null) {
            return;
        }

        tempDir = File.createTempFile("blueprints-accumulo-temp", Long.toString(System.nanoTime()));
        tempDir.delete();
        tempDir.mkdir();
        LOGGER.info("writing to: " + tempDir);

        MiniAccumuloConfig miniAccumuloConfig = new MiniAccumuloConfig(tempDir, ACCUMULO_PASSWORD);
        accumulo = new MiniAccumuloCluster(miniAccumuloConfig);
        accumulo.start();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                try {
                    TestAccumuloCluster.stop();
                } catch (Exception e) {
                    System.out.println("Failed to stop Accumulo test cluster");
                }
            }
        });

        Map configMap = new HashMap();
        configMap.put(AccumuloGraphConfiguration.ZOOKEEPER_SERVERS, accumulo.getZooKeepers());
        configMap.put(AccumuloGraphConfiguration.ACCUMULO_INSTANCE_NAME, accumulo.getInstanceName());
        configMap.put(AccumuloGraphConfiguration.ACCUMULO_USERNAME, ACCUMULO_USERNAME);
        configMap.put(AccumuloGraphConfiguration.ACCUMULO_PASSWORD, ACCUMULO_PASSWORD);
        configMap.put(AccumuloGraphConfiguration.AUTO_FLUSH, true);
        configMap.put(AccumuloGraphConfiguration.MAX_STREAMING_PROPERTY_VALUE_TABLE_DATA_SIZE, GraphTestBase.LARGE_PROPERTY_VALUE_SIZE - 1);
        configMap.put(AccumuloGraphConfiguration.DATA_DIR, "/tmp/");
        config = new AccumuloGraphConfiguration(configMap);
    }

    private static void stop() throws IOException, InterruptedException {
        if (accumulo != null) {
            accumulo.stop();
            accumulo = null;
        }
        tempDir.delete();
    }
}
