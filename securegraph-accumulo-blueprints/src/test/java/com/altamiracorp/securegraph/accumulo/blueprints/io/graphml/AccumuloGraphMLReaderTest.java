package com.altamiracorp.securegraph.accumulo.blueprints.io.graphml;

import com.altamiracorp.securegraph.accumulo.blueprints.util.AccumuloBlueprintsGraphTestHelper;
import com.altamiracorp.securegraph.blueprints.io.graphml.SecureGraphBlueprintsGraphMLReaderTestBase;
import com.tinkerpop.blueprints.impls.GraphTest;
import com.tinkerpop.blueprints.util.io.graphml.GraphMLReaderTestSuite;

public class AccumuloGraphMLReaderTest extends SecureGraphBlueprintsGraphMLReaderTestBase {
    public AccumuloGraphMLReaderTest(GraphTest graphTest) {
        super(graphTest);
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        ((AccumuloBlueprintsGraphTestHelper) this.graphTest).setUp();
    }
}
