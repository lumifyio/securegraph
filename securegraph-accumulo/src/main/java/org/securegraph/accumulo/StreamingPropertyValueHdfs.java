package org.securegraph.accumulo;

import org.securegraph.SecureGraphException;
import org.securegraph.property.StreamingPropertyValue;
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
    public long getLength() {
        try {
            return fs.getFileStatus(path).getLen();
        } catch (IOException ex) {
            throw new SecureGraphException("Could not get length of: " + this.path, ex);
        }
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
