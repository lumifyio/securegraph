package com.altamiracorp.securegraph.blueprints;

import com.tinkerpop.blueprints.TransactionalGraphTestSuite;
import com.tinkerpop.blueprints.impls.GraphTest;

public abstract class SecureGraphBlueprintsTransactionGraphTestBase extends TransactionalGraphTestSuite {
    protected SecureGraphBlueprintsTransactionGraphTestBase(GraphTest graphTest) {
        super(graphTest);
    }
}
