package org.securegraph.accumulo.blueprints;

import org.securegraph.accumulo.blueprints.util.AccumuloBlueprintsGraphTestHelper;
import org.securegraph.blueprints.SecureGraphBlueprintsGraphQueryTestBase;

public class AccumuloGraphQueryTest extends SecureGraphBlueprintsGraphQueryTestBase {
    public AccumuloGraphQueryTest() {
        super(new AccumuloBlueprintsGraphTestHelper());
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        ((AccumuloBlueprintsGraphTestHelper) this.graphTest).setUp();
    }
}
