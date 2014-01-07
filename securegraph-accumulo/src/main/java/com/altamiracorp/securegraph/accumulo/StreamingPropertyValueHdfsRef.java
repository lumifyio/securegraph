package com.altamiracorp.securegraph.accumulo;

import com.altamiracorp.securegraph.property.StreamingPropertyValue;
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

    @Override
    public StreamingPropertyValue toStreamingPropertyValue(AccumuloGraph graph) {
        return new StreamingPropertyValueHdfs(graph.getFileSystem(), new Path(getPath()), getValueType());
    }
}
