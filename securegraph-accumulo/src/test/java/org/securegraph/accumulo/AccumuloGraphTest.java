package org.securegraph.accumulo;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.securegraph.Authorizations;
import org.securegraph.Graph;
import org.securegraph.SecureGraphException;
import org.securegraph.Vertex;
import org.securegraph.test.GraphTestBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.fail;
import static org.junit.Assert.assertNotEquals;

@RunWith(JUnit4.class)
public class AccumuloGraphTest extends GraphTestBase {
    private static final Logger LOGGER = LoggerFactory.getLogger(AccumuloGraphTest.class);
    private final String ACCUMULO_USERNAME = "root";
    private final String ACCUMULO_PASSWORD = "test";
    private File tempDir;
    private static MiniAccumuloCluster accumulo;
    private static AccumuloGraphConfiguration config;

    @Override
    protected Graph createGraph() throws AccumuloSecurityException, AccumuloException, SecureGraphException, InterruptedException, IOException, URISyntaxException {
        return AccumuloGraph.create(config);
    }

    @Override
    protected Authorizations createAuthorizations(String... auths) {
        return new AccumuloAuthorizations(auths);
    }

    @Before
    @Override
    public void before() throws Exception {
        ensureAccumuloIsStarted();
        Connector connector = config.createConnector();
        ensureTableExists(connector, AccumuloGraphConfiguration.DEFAULT_TABLE_NAME_PREFIX);
        dropGraph(connector, AccumuloGraph.getDataTableName(AccumuloGraphConfiguration.DEFAULT_TABLE_NAME_PREFIX));
        dropGraph(connector, AccumuloGraph.getVerticesTableName(AccumuloGraphConfiguration.DEFAULT_TABLE_NAME_PREFIX));
        dropGraph(connector, AccumuloGraph.getEdgesTableName(AccumuloGraphConfiguration.DEFAULT_TABLE_NAME_PREFIX));
        connector.securityOperations().changeUserAuthorizations(AccumuloGraphConfiguration.DEFAULT_ACCUMULO_USERNAME, new org.apache.accumulo.core.security.Authorizations("a", "b", "c", "MIXEDCASE_a"));
        super.before();
    }

    @After
    public void after() throws Exception {
        super.after();
    }

    @Test
    public void testStoringEmptyMetadata() {
        Vertex v1 = graph.addVertex("v1", VISIBILITY_EMPTY, AUTHORIZATIONS_EMPTY);
        Map<String, Object> metadata = new HashMap<String, Object>();
        v1.addPropertyValue("prop1", "prop1", "val1", metadata, VISIBILITY_EMPTY, AUTHORIZATIONS_A_AND_B);

        Vertex v2 = graph.addVertex("v2", VISIBILITY_EMPTY, AUTHORIZATIONS_EMPTY);
        metadata = new HashMap<String, Object>();
        metadata.put("meta1", "metavalue1");
        v2.addPropertyValue("prop1", "prop1", "val1", metadata, VISIBILITY_EMPTY, AUTHORIZATIONS_A_AND_B);

        v1 = graph.getVertex("v1", AUTHORIZATIONS_EMPTY);
        assertEquals(0, v1.getProperty("prop1", "prop1").getMetadata().size());

        v2 = graph.getVertex("v2", AUTHORIZATIONS_EMPTY);
        metadata = v2.getProperty("prop1", "prop1").getMetadata();
        assertEquals(1, metadata.size());
        assertEquals("metavalue1", metadata.get("meta1"));

        AccumuloGraph accumuloGraph = (AccumuloGraph) graph;
        Scanner vertexScanner = accumuloGraph.createVertexScanner(AUTHORIZATIONS_EMPTY);
        vertexScanner.setRange(new Range("V", "W"));
        RowIterator rows = new RowIterator(vertexScanner.iterator());
        while (rows.hasNext()) {
            Iterator<Map.Entry<Key, Value>> row = rows.next();
            while (row.hasNext()) {
                Map.Entry<Key, Value> col = row.next();
                if (col.getKey().getColumnFamily().equals(AccumuloElement.CF_PROPERTY_METADATA)) {
                    if (col.getKey().getRow().toString().equals("Vv1")) {
                        assertEquals("", col.getValue().toString());
                    } else if (col.getKey().getRow().toString().equals("Vv2")) {
                        assertNotEquals("", col.getValue().toString());
                    } else {
                        fail("invalid vertex");
                    }
                }
            }
        }
    }

    public void start() throws IOException, InterruptedException {
        if (accumulo != null) {
            return;
        }

        LOGGER.info("Starting accumulo");

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
                    AccumuloGraphTest.this.stop();
                } catch (Exception e) {
                    System.out.println("Failed to stop Accumulo test cluster");
                }
            }
        });

        Map configMap = createConfig();
        config = new AccumuloGraphConfiguration(configMap);
    }

    protected Map createConfig() {
        Map configMap = new HashMap();
        configMap.put(AccumuloGraphConfiguration.ZOOKEEPER_SERVERS, accumulo.getZooKeepers());
        configMap.put(AccumuloGraphConfiguration.ACCUMULO_INSTANCE_NAME, accumulo.getInstanceName());
        configMap.put(AccumuloGraphConfiguration.ACCUMULO_USERNAME, ACCUMULO_USERNAME);
        configMap.put(AccumuloGraphConfiguration.ACCUMULO_PASSWORD, ACCUMULO_PASSWORD);
        configMap.put(AccumuloGraphConfiguration.AUTO_FLUSH, true);
        configMap.put(AccumuloGraphConfiguration.MAX_STREAMING_PROPERTY_VALUE_TABLE_DATA_SIZE, GraphTestBase.LARGE_PROPERTY_VALUE_SIZE - 1);
        configMap.put(AccumuloGraphConfiguration.DATA_DIR, "/tmp/");
        return configMap;
    }

    private void stop() throws IOException, InterruptedException {
        if (accumulo != null) {
            LOGGER.info("Stopping accumulo");
            accumulo.stop();
            accumulo = null;
        }
        tempDir.delete();
    }

    public void dropGraph(Connector connector, String graphDirectoryName) {
        try {
            if (connector.tableOperations().exists(graphDirectoryName)) {
                connector.tableOperations().delete(graphDirectoryName);
            }
            connector.tableOperations().create(graphDirectoryName);
        } catch (Exception e) {
            throw new RuntimeException("Unable to drop graph: " + graphDirectoryName, e);
        }
    }

    private void ensureTableExists(Connector connector, String tableName) {
        try {
            if (!connector.tableOperations().exists(tableName)) {
                connector.tableOperations().create(tableName);
            }
        } catch (Exception e) {
            throw new RuntimeException("Unable to create table " + tableName);
        }
    }

    private void ensureAccumuloIsStarted() {
        try {
            start();
        } catch (Exception e) {
            throw new RuntimeException("Failed to start Accumulo mini cluster", e);
        }
    }
}
