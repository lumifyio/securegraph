package org.securegraph.accumulo.helpers;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.securegraph.Graph;
import org.securegraph.SecureGraphException;
import org.securegraph.accumulo.AccumuloGraph;
import org.securegraph.accumulo.AccumuloGraphConfiguration;

import java.io.IOException;
import java.net.URISyntaxException;

public class TestHelpers {
    public static Graph createGraph() throws AccumuloSecurityException, AccumuloException, SecureGraphException, InterruptedException, IOException, URISyntaxException {
        return AccumuloGraph.create(TestAccumuloCluster.getConfig());
    }

    public static void before() throws AccumuloSecurityException, AccumuloException {
        ensureAccumuloIsStarted();
        Connector connector = TestAccumuloCluster.getConfig().createConnector();
        ensureTableExists(connector, AccumuloGraphConfiguration.DEFAULT_TABLE_NAME_PREFIX);
        dropGraph(connector, AccumuloGraph.getDataTableName(AccumuloGraphConfiguration.DEFAULT_TABLE_NAME_PREFIX));
        dropGraph(connector, AccumuloGraph.getVerticesTableName(AccumuloGraphConfiguration.DEFAULT_TABLE_NAME_PREFIX));
        dropGraph(connector, AccumuloGraph.getEdgesTableName(AccumuloGraphConfiguration.DEFAULT_TABLE_NAME_PREFIX));
        connector.securityOperations().changeUserAuthorizations(AccumuloGraphConfiguration.DEFAULT_ACCUMULO_USERNAME, new Authorizations("a", "b", "c"));
    }

    public static Graph clearGraph(Graph graph) throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
        AccumuloGraph accumuloGraph = (AccumuloGraph) graph;
        accumuloGraph.getConnector().tableOperations().deleteRows(AccumuloGraph.getDataTableName(AccumuloGraphConfiguration.DEFAULT_TABLE_NAME_PREFIX), new Text("a"), new Text("z"));
        accumuloGraph.getConnector().tableOperations().deleteRows(AccumuloGraph.getVerticesTableName(AccumuloGraphConfiguration.DEFAULT_TABLE_NAME_PREFIX), new Text("a"), new Text("z"));
        accumuloGraph.getConnector().tableOperations().deleteRows(AccumuloGraph.getEdgesTableName(AccumuloGraphConfiguration.DEFAULT_TABLE_NAME_PREFIX), new Text("a"), new Text("z"));
        return graph;
    }

    public static void after() {
    }

    public static void dropGraph(Connector connector, String graphDirectoryName) {
        try {
            if (connector.tableOperations().exists(graphDirectoryName)) {
                connector.tableOperations().delete(graphDirectoryName);
            }
            connector.tableOperations().create(graphDirectoryName);
        } catch (Exception e) {
            throw new RuntimeException("Unable to drop graph: " + graphDirectoryName, e);
        }
    }

    private static void ensureTableExists(Connector connector, String tableName) {
        try {
            if (!connector.tableOperations().exists(tableName)) {
                connector.tableOperations().create(tableName);
            }
        } catch (Exception e) {
            throw new RuntimeException("Unable to create table " + tableName);
        }
    }

    private static void ensureAccumuloIsStarted() {
        try {
            TestAccumuloCluster.start();
        } catch (Exception e) {
            throw new RuntimeException("Failed to start Accumulo mini cluster", e);
        }
    }
}
