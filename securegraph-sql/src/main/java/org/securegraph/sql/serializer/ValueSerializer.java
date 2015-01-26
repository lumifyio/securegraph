package org.securegraph.sql.serializer;

public interface ValueSerializer {
    <T> T toObject(byte[] value);

    byte[] toBytes(Object value);
}
