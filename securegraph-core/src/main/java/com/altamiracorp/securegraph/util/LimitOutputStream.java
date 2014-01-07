package com.altamiracorp.securegraph.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class LimitOutputStream extends OutputStream {
    private final int maxSizeToStore;
    private final ByteArrayOutputStream smallOutputStream;
    private final LargeDataStore largeDataStore;
    private OutputStream largeOutputStream;
    private long length;

    public LimitOutputStream(LargeDataStore largeDataStore, long maxSizeToStore) {
        this.largeDataStore = largeDataStore;
        this.maxSizeToStore = (int) maxSizeToStore;
        this.smallOutputStream = new ByteArrayOutputStream((int) maxSizeToStore);
        this.length = 0;
    }

    private synchronized OutputStream getLargeOutputStream() throws IOException {
        if (largeOutputStream == null) {
            largeOutputStream = largeDataStore.createOutputStream();
            if (smallOutputStream.size() > 0) {
                largeOutputStream.write(smallOutputStream.toByteArray());
            }
        }
        return largeOutputStream;
    }

    @Override
    public synchronized void write(int b) throws IOException {
        if (this.smallOutputStream.size() <= maxSizeToStore - 1) {
            this.smallOutputStream.write(b);
        } else {
            getLargeOutputStream().write(b);
        }
        length++;
    }

    @Override
    public synchronized void write(byte[] b) throws IOException {
        if (this.smallOutputStream.size() <= maxSizeToStore - b.length) {
            this.smallOutputStream.write(b);
        } else {
            getLargeOutputStream().write(b);
        }
        length += b.length;
    }

    @Override
    public synchronized void write(byte[] b, int off, int len) throws IOException {
        if (this.smallOutputStream.size() <= maxSizeToStore - len) {
            this.smallOutputStream.write(b, off, len);
        } else {
            getLargeOutputStream().write(b, off, len);
        }
        length += len;
    }

    public synchronized boolean hasExceededSizeLimit() {
        return this.largeOutputStream != null;
    }

    public byte[] getSmall() {
        if (hasExceededSizeLimit()) {
            return null;
        }
        return this.smallOutputStream.toByteArray();
    }

    @Override
    public synchronized void flush() throws IOException {
        if (this.largeOutputStream != null) {
            this.largeOutputStream.flush();
        }
        super.close();
    }

    @Override
    public synchronized void close() throws IOException {
        if (this.largeOutputStream != null) {
            this.largeOutputStream.close();
        }
        if (this.smallOutputStream != null) {
            this.smallOutputStream.close();
        }
        super.close();
    }

    public long getLength() {
        return this.length;
    }

    public static abstract class LargeDataStore {
        public abstract OutputStream createOutputStream() throws IOException;
    }
}
