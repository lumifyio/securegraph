package com.altamiracorp.securegraph.accumulo.blueprints.io.graphson;

import com.altamiracorp.securegraph.accumulo.blueprints.util.AccumuloBlueprintsGraphTestHelper;
import com.altamiracorp.securegraph.blueprints.io.graphson.SecureGraphBlueprintsGraphSONReaderTestBase;

public class AccumuloGraphSONReaderTest extends SecureGraphBlueprintsGraphSONReaderTestBase {
    public AccumuloGraphSONReaderTest() {
        super(new AccumuloBlueprintsGraphTestHelper());
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        ((AccumuloBlueprintsGraphTestHelper) this.graphTest).setUp();
    }
}
