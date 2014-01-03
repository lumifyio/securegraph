package com.altamiracorp.securegraph.accumulo.blueprints.io.gml;

import com.altamiracorp.securegraph.accumulo.blueprints.util.AccumuloBlueprintsGraphTestHelper;
import com.altamiracorp.securegraph.blueprints.io.gml.SecureGraphBlueprintsGMLReaderTestBase;
import com.tinkerpop.blueprints.impls.GraphTest;
import com.tinkerpop.blueprints.util.io.gml.GMLReaderTestSuite;

public class AccumuloGMLReaderTest extends SecureGraphBlueprintsGMLReaderTestBase {
    public AccumuloGMLReaderTest(GraphTest graphTest) {
        super(graphTest);
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        ((AccumuloBlueprintsGraphTestHelper) this.graphTest).setUp();
    }
}
