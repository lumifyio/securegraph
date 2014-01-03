package com.altamiracorp.securegraph.accumulo.blueprints.io.graphson;

import com.altamiracorp.securegraph.accumulo.blueprints.util.AccumuloBlueprintsGraphTestHelper;
import com.altamiracorp.securegraph.blueprints.io.graphson.SecureGraphBlueprintsGraphSONReaderTestBase;
import com.tinkerpop.blueprints.impls.GraphTest;

public class AccumuloGraphSONReaderTest extends SecureGraphBlueprintsGraphSONReaderTestBase {
    public AccumuloGraphSONReaderTest(GraphTest graphTest) {
        super(graphTest);
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        ((AccumuloBlueprintsGraphTestHelper) this.graphTest).setUp();
    }
}
