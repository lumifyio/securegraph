package com.altamiracorp.securegraph.accumulo;

import com.altamiracorp.securegraph.property.StreamingPropertyValue;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

class StreamingPropertyValueTable extends StreamingPropertyValue {
    private final AccumuloGraph graph;
    private final String dataRowKey;
    private final transient byte[] data;

    StreamingPropertyValueTable(AccumuloGraph graph, String dataRowKey, StreamingPropertyValueTableRef valueRef) {
        super(null, valueRef.getValueType());
        this.store(valueRef.isStore());
        this.searchIndex(valueRef.isSearchIndex());
        this.graph = graph;
        this.dataRowKey = dataRowKey;
        this.data = valueRef.getData();
    }

    @Override
    public InputStream getInputStream() {
        // we need to store the data here to handle the case that the mutation hasn't been flushed yet but the element is
        // passed to the search indexer to be indexed and we can't get the value yet.
        if (this.data != null) {
            return new ByteArrayInputStream(this.data);
        }
        return new ByteArrayInputStream(this.graph.streamingPropertyValueTableData(this.dataRowKey));
    }
}
