package com.altamiracorp.securegraph.blueprints;

import com.tinkerpop.blueprints.IndexTestSuite;
import com.tinkerpop.blueprints.impls.GraphTest;

public abstract class SecureGraphBlueprintsIndexTestBase extends IndexTestSuite {
    protected SecureGraphBlueprintsIndexTestBase(GraphTest graphTest) {
        super(graphTest);
    }
}
