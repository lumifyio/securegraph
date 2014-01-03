package com.altamiracorp.securegraph.blueprints;

import com.tinkerpop.blueprints.VertexTestSuite;
import com.tinkerpop.blueprints.impls.GraphTest;

public abstract class SecureGraphBlueprintsVertexTestBase extends VertexTestSuite {
    protected SecureGraphBlueprintsVertexTestBase(GraphTest graphTest) {
        super(graphTest);
    }
}
