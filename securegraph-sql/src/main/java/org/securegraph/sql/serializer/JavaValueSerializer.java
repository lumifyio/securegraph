package org.securegraph.sql.serializer;

import org.securegraph.util.JavaSerializableUtils;

public class JavaValueSerializer implements ValueSerializer {
    @Override
    public <T> T toObject(byte[] value) {
        return (T) JavaSerializableUtils.bytesToObject(value);
    }

    @Override
    public byte[] toBytes(Object value) {
        return JavaSerializableUtils.objectToBytes(value);
    }
}
