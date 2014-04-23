package org.securegraph.accumulo.blueprints;

import org.securegraph.accumulo.blueprints.util.AccumuloBlueprintsGraphTestHelper;
import org.securegraph.blueprints.SecureGraphBlueprintsEdgeTestBase;

public class AccumuloEdgeTest extends SecureGraphBlueprintsEdgeTestBase {
    public AccumuloEdgeTest() {
        super(new AccumuloBlueprintsGraphTestHelper());
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        ((AccumuloBlueprintsGraphTestHelper) this.graphTest).setUp();
    }
}
