package com.altamiracorp.securegraph.accumulo;

import com.altamiracorp.securegraph.property.StreamingPropertyValue;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

class StreamingPropertyValueTable extends StreamingPropertyValue {
    private final AccumuloGraph graph;
    private final String dataRowKey;

    StreamingPropertyValueTable(AccumuloGraph graph, String dataRowKey, StreamingPropertyValueRef streamingPropertyValueRef) {
        super(null, streamingPropertyValueRef.getValueType());
        this.store(streamingPropertyValueRef.isStore());
        this.searchIndex(streamingPropertyValueRef.isSearchIndex());
        this.graph = graph;
        this.dataRowKey = dataRowKey;
    }

    @Override
    public InputStream getInputStream() {
        return new ByteArrayInputStream(this.graph.streamingPropertyValueTableData(this.dataRowKey));
    }
}
