package com.altamiracorp.securegraph.accumulo.blueprints;

import com.altamiracorp.securegraph.accumulo.blueprints.util.AccumuloBlueprintsGraphTestHelper;
import com.altamiracorp.securegraph.blueprints.SecureGraphBlueprintsIndexTestBase;
import org.junit.Ignore;

@Ignore
public class AccumuloIndexTest extends SecureGraphBlueprintsIndexTestBase {
    public AccumuloIndexTest() {
        super(new AccumuloBlueprintsGraphTestHelper());
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        ((AccumuloBlueprintsGraphTestHelper) this.graphTest).setUp();
    }
}
