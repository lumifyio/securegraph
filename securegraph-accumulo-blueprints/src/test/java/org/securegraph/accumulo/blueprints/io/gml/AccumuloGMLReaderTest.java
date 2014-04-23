package org.securegraph.accumulo.blueprints.io.gml;

import org.securegraph.accumulo.blueprints.util.AccumuloBlueprintsGraphTestHelper;
import org.securegraph.blueprints.io.gml.SecureGraphBlueprintsGMLReaderTestBase;

public class AccumuloGMLReaderTest extends SecureGraphBlueprintsGMLReaderTestBase {
    public AccumuloGMLReaderTest() {
        super(new AccumuloBlueprintsGraphTestHelper());
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        ((AccumuloBlueprintsGraphTestHelper) this.graphTest).setUp();
    }
}
