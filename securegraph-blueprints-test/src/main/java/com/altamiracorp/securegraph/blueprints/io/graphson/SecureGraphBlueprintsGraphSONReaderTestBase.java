package com.altamiracorp.securegraph.blueprints.io.graphson;

import com.tinkerpop.blueprints.impls.GraphTest;
import com.tinkerpop.blueprints.util.io.graphson.GraphSONReaderTestSuite;

public abstract class SecureGraphBlueprintsGraphSONReaderTestBase extends GraphSONReaderTestSuite {
    protected SecureGraphBlueprintsGraphSONReaderTestBase(GraphTest graphTest) {
        super(graphTest);
    }
}
