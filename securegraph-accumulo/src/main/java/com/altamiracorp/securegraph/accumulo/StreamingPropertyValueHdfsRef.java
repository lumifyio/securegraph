package com.altamiracorp.securegraph.accumulo;

import org.apache.hadoop.fs.Path;

class StreamingPropertyValueHdfsRef extends StreamingPropertyValueRef {
    private final String path;

    public StreamingPropertyValueHdfsRef(Path path, Class valueType) {
        super(valueType);
        this.path = path.toString();
    }

    public String getPath() {
        return path;
    }
}
