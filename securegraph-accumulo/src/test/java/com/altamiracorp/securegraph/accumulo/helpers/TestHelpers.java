package com.altamiracorp.securegraph.accumulo.helpers;

import com.altamiracorp.securegraph.Graph;
import com.altamiracorp.securegraph.SecureGraphException;
import com.altamiracorp.securegraph.accumulo.AccumuloGraph;
import com.altamiracorp.securegraph.accumulo.AccumuloGraphConfiguration;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.security.Authorizations;

public class TestHelpers {
    public static Graph createGraph() throws AccumuloSecurityException, AccumuloException, SecureGraphException {
        return AccumuloGraph.create(TestAccumuloCluster.getConfig());
    }

    public static void before() throws AccumuloSecurityException, AccumuloException {
        ensureAccumuloIsStarted();
        Connector connector = TestAccumuloCluster.getConfig().createConnector();
        ensureTableExists(connector, AccumuloGraphConfiguration.DEFAULT_TABLE_NAME);
        connector.securityOperations().changeUserAuthorizations(AccumuloGraphConfiguration.DEFAULT_ACCUMULO_USERNAME, new Authorizations("a", "b", "c"));
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
