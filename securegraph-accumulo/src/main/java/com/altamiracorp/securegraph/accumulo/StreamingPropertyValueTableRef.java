package com.altamiracorp.securegraph.accumulo;

import com.altamiracorp.securegraph.property.StreamingPropertyValue;

class StreamingPropertyValueTableRef extends StreamingPropertyValueRef {
    private final String dataRowKey;

    public StreamingPropertyValueTableRef(String dataRowKey, StreamingPropertyValue propertyValue) {
        super(propertyValue);
        this.dataRowKey = dataRowKey;
    }

    public String getDataRowKey() {
        return dataRowKey;
    }

    @Override
    public StreamingPropertyValue toStreamingPropertyValue(AccumuloGraph graph) {
        return new StreamingPropertyValueTable(graph, getDataRowKey(), this);
    }
}
