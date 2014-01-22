package com.altamiracorp.securegraph.accumulo;

import com.altamiracorp.securegraph.SecureGraphException;
import com.altamiracorp.securegraph.property.StreamingPropertyValue;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.InputStream;

class StreamingPropertyValueHdfs extends StreamingPropertyValue {
    private final FileSystem fs;
    private final Path path;

    public StreamingPropertyValueHdfs(FileSystem fs, Path path, StreamingPropertyValueRef streamingPropertyValueRef) {
        super(null, streamingPropertyValueRef.getValueType());
        this.store(streamingPropertyValueRef.isStore());
        this.searchIndex(streamingPropertyValueRef.isSearchIndex());
        this.fs = fs;
        this.path = path;
    }

    @Override
    public InputStream getInputStream() {
        try {
            return fs.open(this.path);
        } catch (IOException ex) {
            throw new SecureGraphException("Could not open: " + this.path, ex);
        }
    }
}
