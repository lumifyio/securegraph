package com.altamiracorp.securegraph.accumulo.blueprints;

import com.altamiracorp.securegraph.accumulo.blueprints.util.AccumuloBlueprintsGraphTestHelper;
import com.altamiracorp.securegraph.blueprints.SecureGraphBlueprintsKeyIndexableGraphTestBase;
import org.junit.Ignore;

@Ignore
public class AccumuloKeyIndexableGraphTest extends SecureGraphBlueprintsKeyIndexableGraphTestBase {
    public AccumuloKeyIndexableGraphTest() {
        super(new AccumuloBlueprintsGraphTestHelper());
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        ((AccumuloBlueprintsGraphTestHelper) this.graphTest).setUp();
    }
}
