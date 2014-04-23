package org.securegraph.accumulo;

import org.securegraph.property.StreamingPropertyValue;

class StreamingPropertyValueTableRef extends StreamingPropertyValueRef {
    private final String dataRowKey;
    private final transient byte[] data;

    public StreamingPropertyValueTableRef(String dataRowKey, StreamingPropertyValue propertyValue, byte[] data) {
        super(propertyValue);
        this.dataRowKey = dataRowKey;
        this.data = data;
    }

    public String getDataRowKey() {
        return dataRowKey;
    }

    public byte[] getData() {
        return data;
    }

    @Override
    public StreamingPropertyValue toStreamingPropertyValue(AccumuloGraph graph) {
        return new StreamingPropertyValueTable(graph, getDataRowKey(), this);
    }
}
