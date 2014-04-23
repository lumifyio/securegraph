package org.securegraph.blueprints;

import com.tinkerpop.blueprints.VertexQueryTestSuite;
import com.tinkerpop.blueprints.impls.GraphTest;

public abstract class SecureGraphBlueprintsVertexQueryTestBase extends VertexQueryTestSuite {
    protected SecureGraphBlueprintsVertexQueryTestBase(GraphTest graphTest) {
        super(graphTest);
    }
}
