package com.altamiracorp.securegraph.accumulo.blueprints.util;

import com.altamiracorp.securegraph.Authorizations;
import com.altamiracorp.securegraph.accumulo.AccumuloAuthorizations;
import com.altamiracorp.securegraph.accumulo.AccumuloGraph;
import com.altamiracorp.securegraph.accumulo.AccumuloGraphConfiguration;
import com.altamiracorp.securegraph.accumulo.blueprints.AccumuloSecureGraphBlueprintsGraph;
import com.altamiracorp.securegraph.blueprints.AuthorizationsProvider;
import com.altamiracorp.securegraph.blueprints.DefaultVisibilityProvider;
import com.altamiracorp.securegraph.blueprints.VisibilityProvider;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.TestSuite;
import com.tinkerpop.blueprints.impls.GraphTest;
import org.apache.accumulo.core.client.Connector;
import org.junit.Ignore;

import java.util.HashMap;

@Ignore
public class AccumuloBlueprintsGraphTestHelper extends GraphTest {
    private final AccumuloSecureGraphBlueprintsGraph defaultGraph;
    private final VisibilityProvider visibilityProvider = new DefaultVisibilityProvider(new HashMap());
    private final AuthorizationsProvider authorizationsProvider = new AuthorizationsProvider() {
        @Override
        public Authorizations getAuthorizations() {
            return new AccumuloAuthorizations();
        }
    };

    public AccumuloBlueprintsGraphTestHelper() {
        try {
            this.ensureAccumuloIsStarted();
            AccumuloGraphConfiguration config = this.getGraphConfig(AccumuloGraphConfiguration.DEFAULT_TABLE_NAME_PREFIX);
            this.defaultGraph = new AccumuloSecureGraphBlueprintsGraph(AccumuloGraph.create(config), visibilityProvider, authorizationsProvider);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public Graph generateGraph() {
        return this.defaultGraph;
    }

    @Override
    public Graph generateGraph(String graphDirectoryName) {
        try {
            AccumuloGraphConfiguration config = this.getGraphConfig(graphDirectoryName);
            return new AccumuloSecureGraphBlueprintsGraph(AccumuloGraph.create(config), visibilityProvider, authorizationsProvider);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void doTestSuite(TestSuite testSuite) throws Exception {
        throw new RuntimeException("not implemented");
    }

    public void setUp() {
        dropGraph(AccumuloGraphConfiguration.DEFAULT_TABLE_NAME_PREFIX);
    }

    @Override
    public void dropGraph(String graphDirectoryName) {
        try {
            Connector connector = getConnector(graphDirectoryName);
            if (connector.tableOperations().exists(graphDirectoryName + "_d")) {
                connector.tableOperations().delete(graphDirectoryName + "_d");
            }
            if (connector.tableOperations().exists(graphDirectoryName + "_v")) {
                connector.tableOperations().delete(graphDirectoryName + "_v");
            }
            if (connector.tableOperations().exists(graphDirectoryName + "_e")) {
                connector.tableOperations().delete(graphDirectoryName + "_e");
            }
            connector.tableOperations().create(graphDirectoryName + "_d");
            connector.tableOperations().create(graphDirectoryName + "_v");
            connector.tableOperations().create(graphDirectoryName + "_e");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public Connector getConnector(String graphDirectoryName) {
        try {
            return this.getGraphConfig(graphDirectoryName).createConnector();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void ensureAccumuloIsStarted() {
        try {
            TestAccumuloCluster.start();
        } catch (Exception e) {
            throw new RuntimeException("Failed to start Accumulo mini cluster", e);
        }
    }

    private AccumuloGraphConfiguration getGraphConfig(String tableName) {
        AccumuloGraphConfiguration config = TestAccumuloCluster.getConfig();
        config.set(AccumuloGraphConfiguration.DEFAULT_TABLE_NAME_PREFIX, tableName);
        return config;
    }
}
