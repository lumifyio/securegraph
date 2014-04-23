package org.securegraph.blueprints;

import com.tinkerpop.blueprints.EdgeTestSuite;
import com.tinkerpop.blueprints.impls.GraphTest;

public abstract class SecureGraphBlueprintsEdgeTestBase extends EdgeTestSuite {
    protected SecureGraphBlueprintsEdgeTestBase(GraphTest graphTest) {
        super(graphTest);
    }
}
