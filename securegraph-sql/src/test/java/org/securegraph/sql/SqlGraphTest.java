package org.securegraph.sql;

import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.securegraph.Authorizations;
import org.securegraph.Graph;
import org.securegraph.test.GraphTestBase;

import java.util.HashMap;
import java.util.Map;

@RunWith(JUnit4.class)
public class SqlGraphTest extends GraphTestBase {
    @Override
    protected Graph createGraph() throws Exception {
        Map configMap = new HashMap();
        configMap.put(SqlGraphConfiguration.JDBC_CLASS_NAME, "org.h2.Driver");
        configMap.put(SqlGraphConfiguration.JDBC_URL, "jdbc:h2:mem:test");
        SqlGraphConfiguration config = new SqlGraphConfiguration(configMap);
        return SqlGraph.create(config);
    }

    @Override
    protected Authorizations createAuthorizations(String... auths) {
        return new SqlAuthorizations(auths);
    }

    @Override
    protected boolean isEdgeBoostSupported() {
        return false;
    }
}
