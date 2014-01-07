package com.altamiracorp.securegraph.accumulo;

import com.altamiracorp.securegraph.Authorizations;
import com.altamiracorp.securegraph.property.StreamingPropertyValue;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

class StreamingPropertyValueTable extends StreamingPropertyValue {
    private final AccumuloGraph graph;
    private final String dataRowKey;

    StreamingPropertyValueTable(AccumuloGraph graph, String dataRowKey, Class valueType) {
        super(null, valueType);
        this.graph = graph;
        this.dataRowKey = dataRowKey;
    }

    @Override
    public InputStream getInputStream(Authorizations authorizations) {
        return new ByteArrayInputStream(this.graph.streamingPropertyValueTableData(this.dataRowKey, authorizations));
    }
}
