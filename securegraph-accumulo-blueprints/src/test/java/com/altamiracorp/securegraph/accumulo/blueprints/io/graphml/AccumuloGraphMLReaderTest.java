package com.altamiracorp.securegraph.accumulo.blueprints.io.graphml;

import com.altamiracorp.securegraph.accumulo.blueprints.util.AccumuloBlueprintsGraphTestHelper;
import com.altamiracorp.securegraph.blueprints.io.graphml.SecureGraphBlueprintsGraphMLReaderTestBase;

public class AccumuloGraphMLReaderTest extends SecureGraphBlueprintsGraphMLReaderTestBase {
    public AccumuloGraphMLReaderTest() {
        super(new AccumuloBlueprintsGraphTestHelper());
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        ((AccumuloBlueprintsGraphTestHelper) this.graphTest).setUp();
    }
}
