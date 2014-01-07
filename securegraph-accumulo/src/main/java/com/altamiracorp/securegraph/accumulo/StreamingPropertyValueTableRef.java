package com.altamiracorp.securegraph.accumulo;

class StreamingPropertyValueTableRef extends StreamingPropertyValueRef {
    private final String dataRowKey;

    public StreamingPropertyValueTableRef(String dataRowKey, Class valueType) {
        super(valueType);
        this.dataRowKey = dataRowKey;
    }

    public String getDataRowKey() {
        return dataRowKey;
    }
}
