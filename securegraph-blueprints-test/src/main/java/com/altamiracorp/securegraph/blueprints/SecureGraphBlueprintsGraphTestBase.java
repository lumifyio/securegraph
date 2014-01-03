package com.altamiracorp.securegraph.blueprints;

import com.tinkerpop.blueprints.GraphTestSuite;
import com.tinkerpop.blueprints.impls.GraphTest;

public abstract class SecureGraphBlueprintsGraphTestBase extends GraphTestSuite {
    protected SecureGraphBlueprintsGraphTestBase(GraphTest graphTest) {
        super(graphTest);
    }
}
