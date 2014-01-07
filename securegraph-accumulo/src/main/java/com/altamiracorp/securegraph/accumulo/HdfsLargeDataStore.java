package com.altamiracorp.securegraph.accumulo;

import com.altamiracorp.securegraph.util.LimitOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Random;

class HdfsLargeDataStore extends LimitOutputStream.LargeDataStore {
    private static Random random = new Random();
    private final FileSystem fs;
    private Path fileName;

    public HdfsLargeDataStore(FileSystem fs) {
        this.fs = fs;
    }

    @Override
    public OutputStream createOutputStream() throws IOException {
        fileName = createTempFileName();
        return fs.create(fileName);
    }

    private Path createTempFileName() {
        return new Path("/tmp/" + getClass().getName() + "-" + random.nextLong());
    }

    public Path getFileName() {
        return fileName;
    }
}
