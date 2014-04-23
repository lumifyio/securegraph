package org.securegraph.accumulo;

import org.securegraph.property.StreamingPropertyValue;
import org.apache.hadoop.fs.Path;

class StreamingPropertyValueHdfsRef extends StreamingPropertyValueRef {
    private final String path;

    public StreamingPropertyValueHdfsRef(String path, StreamingPropertyValue propertyValue) {
        super(propertyValue);
        this.path = path;
    }

    public String getPath() {
        return path;
    }

    @Override
    public StreamingPropertyValue toStreamingPropertyValue(AccumuloGraph graph) {
        return new StreamingPropertyValueHdfs(graph.getFileSystem(), new Path(graph.getDataDir(), getPath()), this);
    }
}
