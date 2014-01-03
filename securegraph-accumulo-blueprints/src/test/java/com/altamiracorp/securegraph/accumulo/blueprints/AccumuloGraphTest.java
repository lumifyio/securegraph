package com.altamiracorp.securegraph.accumulo.blueprints;

import com.altamiracorp.securegraph.accumulo.blueprints.util.AccumuloBlueprintsGraphTestHelper;
import com.altamiracorp.securegraph.blueprints.SecureGraphBlueprintsGraphTestBase;

public class AccumuloGraphTest extends SecureGraphBlueprintsGraphTestBase {
    public AccumuloGraphTest() {
        super(new AccumuloBlueprintsGraphTestHelper());
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        ((AccumuloBlueprintsGraphTestHelper) this.graphTest).setUp();
    }

    // TODO turn this back on when performance isn't horrible
    @Override
    public void testTreeConnectivity() {
        assertEquals("turn this back on when performance isn't horrible", true, false);
        //super.testTreeConnectivity();
    }
}
