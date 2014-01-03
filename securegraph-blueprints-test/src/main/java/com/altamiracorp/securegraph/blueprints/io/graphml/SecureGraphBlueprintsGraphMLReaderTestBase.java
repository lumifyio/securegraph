package com.altamiracorp.securegraph.blueprints.io.graphml;

import com.tinkerpop.blueprints.impls.GraphTest;
import com.tinkerpop.blueprints.util.io.graphml.GraphMLReaderTestSuite;

public abstract class SecureGraphBlueprintsGraphMLReaderTestBase extends GraphMLReaderTestSuite {
    protected SecureGraphBlueprintsGraphMLReaderTestBase(GraphTest graphTest) {
        super(graphTest);
    }
}
