package com.altamiracorp.securegraph.accumulo;

import com.altamiracorp.securegraph.Graph;
import com.altamiracorp.securegraph.SecureGraphException;
import com.altamiracorp.securegraph.accumulo.helpers.TestHelpers;
import com.altamiracorp.securegraph.test.GraphTestBase;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class AccumuloGraphTest extends GraphTestBase {
    @Override
    protected Graph createGraph() throws AccumuloSecurityException, AccumuloException, SecureGraphException {
        return TestHelpers.createGraph();
    }

    @Override
    public void before() throws Exception {
        TestHelpers.before();
        super.before();
    }
}
