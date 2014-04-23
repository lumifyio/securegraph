package org.securegraph.blueprints;

import com.tinkerpop.blueprints.GraphQueryTestSuite;
import com.tinkerpop.blueprints.impls.GraphTest;

public abstract class SecureGraphBlueprintsGraphQueryTestBase extends GraphQueryTestSuite {
    protected SecureGraphBlueprintsGraphQueryTestBase(GraphTest graphTest) {
        super(graphTest);
    }
}
