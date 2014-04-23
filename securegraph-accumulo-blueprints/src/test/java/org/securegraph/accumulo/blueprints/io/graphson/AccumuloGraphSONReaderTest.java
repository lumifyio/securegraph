package org.securegraph.accumulo.blueprints.io.graphson;

import org.securegraph.accumulo.blueprints.util.AccumuloBlueprintsGraphTestHelper;
import org.securegraph.blueprints.io.graphson.SecureGraphBlueprintsGraphSONReaderTestBase;

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
