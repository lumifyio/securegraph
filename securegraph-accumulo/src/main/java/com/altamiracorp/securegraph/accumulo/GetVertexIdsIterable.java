package com.altamiracorp.securegraph.accumulo;

import com.altamiracorp.securegraph.util.LookAheadIterable;

import java.util.Collection;
import java.util.Iterator;

class GetVertexIdsIterable extends LookAheadIterable<EdgeInfo, Object> {
    private final Collection<EdgeInfo> edgeInfos;
    private final String[] labels;

    public GetVertexIdsIterable(Collection<EdgeInfo> edgeInfos, String[] labels) {
        this.edgeInfos = edgeInfos;
        this.labels = labels;
    }

    @Override
    protected boolean isIncluded(EdgeInfo edgeInfo, Object vertexId) {
        if (labels == null) {
            return true;
        }
        for (String label : labels) {
            if (edgeInfo.getLabel().equals(label)) {
                return true;
            }
        }
        return false;
    }

    @Override
    protected Object convert(EdgeInfo edgeInfo) {
        return edgeInfo.getVertexId();
    }

    @Override
    protected Iterator<EdgeInfo> createIterator() {
        return edgeInfos.iterator();
    }
}
