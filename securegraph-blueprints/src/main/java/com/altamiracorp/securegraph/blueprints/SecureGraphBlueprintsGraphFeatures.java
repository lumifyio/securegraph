package com.altamiracorp.securegraph.blueprints;

import com.tinkerpop.blueprints.Features;

public class SecureGraphBlueprintsGraphFeatures extends Features {
    public SecureGraphBlueprintsGraphFeatures() {
        ignoresSuppliedIds = false;
        isPersistent = true;
        isWrapper = false;

        supportsBooleanProperty = true;
        supportsDoubleProperty = true;
        supportsFloatProperty = true;
        supportsIntegerProperty = true;
        supportsLongProperty = true;
        supportsMapProperty = true;
        supportsMixedListProperty = true;
        supportsPrimitiveArrayProperty = true;
        supportsSerializableObjectProperty = true;
        supportsUniformListProperty = true;

        supportsDuplicateEdges = true;
        supportsEdgeIndex = false;
        supportsEdgeIteration = true;
        supportsEdgeKeyIndex = true;
        supportsEdgeProperties = true;
        supportsEdgeRetrieval = true;
        supportsIndices = false;
        supportsKeyIndices = true;
        supportsSelfLoops = true;
        supportsStringProperty = true;
        supportsThreadedTransactions = false;
        supportsTransactions = false;
        supportsVertexIndex = false;
        supportsVertexIteration = true;
        supportsVertexKeyIndex = true;
        supportsVertexProperties = true;
    }
}
