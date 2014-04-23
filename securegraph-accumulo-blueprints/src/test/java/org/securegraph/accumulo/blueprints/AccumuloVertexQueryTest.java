package org.securegraph.accumulo.blueprints;

import org.securegraph.accumulo.blueprints.util.AccumuloBlueprintsGraphTestHelper;
import org.securegraph.blueprints.SecureGraphBlueprintsVertexQueryTestBase;

public class AccumuloVertexQueryTest extends SecureGraphBlueprintsVertexQueryTestBase {
    public AccumuloVertexQueryTest() {
        super(new AccumuloBlueprintsGraphTestHelper());
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        ((AccumuloBlueprintsGraphTestHelper) this.graphTest).setUp();
    }
}
