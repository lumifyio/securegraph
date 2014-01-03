package com.altamiracorp.securegraph.blueprints.io.gml;

import com.tinkerpop.blueprints.impls.GraphTest;
import com.tinkerpop.blueprints.util.io.gml.GMLReaderTestSuite;

public abstract class SecureGraphBlueprintsGMLReaderTestBase extends GMLReaderTestSuite {
    protected SecureGraphBlueprintsGMLReaderTestBase(GraphTest graphTest) {
        super(graphTest);
    }
}
