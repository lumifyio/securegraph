package org.securegraph.accumulo.blueprints;

import org.securegraph.accumulo.blueprints.util.AccumuloBlueprintsGraphTestHelper;
import org.securegraph.blueprints.SecureGraphBlueprintsGraphTestBase;

public class AccumuloGraphTest extends SecureGraphBlueprintsGraphTestBase {
    public AccumuloGraphTest() {
        super(new AccumuloBlueprintsGraphTestHelper());
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        ((AccumuloBlueprintsGraphTestHelper) this.graphTest).setUp();
    }
}
