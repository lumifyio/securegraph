package com.altamiracorp.securegraph.accumulo.blueprints;

import com.altamiracorp.securegraph.accumulo.blueprints.util.AccumuloBlueprintsGraphTestHelper;
import com.altamiracorp.securegraph.blueprints.SecureGraphBlueprintsIndexableGraphTestBase;
import org.junit.Ignore;

@Ignore
public class AccumuloIndexableGraphTest extends SecureGraphBlueprintsIndexableGraphTestBase {
    public AccumuloIndexableGraphTest() {
        super(new AccumuloBlueprintsGraphTestHelper());
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        ((AccumuloBlueprintsGraphTestHelper) this.graphTest).setUp();
    }
}
