package com.altamiracorp.securegraph.blueprints;

import com.tinkerpop.blueprints.IndexableGraphTestSuite;
import com.tinkerpop.blueprints.impls.GraphTest;

public abstract class SecureGraphBlueprintsIndexableGraphTestBase extends IndexableGraphTestSuite {
    protected SecureGraphBlueprintsIndexableGraphTestBase(GraphTest graphTest) {
        super(graphTest);
    }
}
