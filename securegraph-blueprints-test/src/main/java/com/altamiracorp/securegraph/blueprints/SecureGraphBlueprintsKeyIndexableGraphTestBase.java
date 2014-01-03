package com.altamiracorp.securegraph.blueprints;

import com.tinkerpop.blueprints.KeyIndexableGraphTestSuite;
import com.tinkerpop.blueprints.impls.GraphTest;

public abstract class SecureGraphBlueprintsKeyIndexableGraphTestBase extends KeyIndexableGraphTestSuite {
    protected SecureGraphBlueprintsKeyIndexableGraphTestBase(GraphTest graphTest) {
        super(graphTest);
    }
}
