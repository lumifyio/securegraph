package com.altamiracorp.securegraph.accumulo.serializer;

import org.apache.accumulo.core.data.Value;

import java.io.*;

public class JavaValueSerializer implements ValueSerializer {
    @Override
    public Value objectToValue(Object value) {
        return new Value(objectToBytes(value));
    }

    @Override
    public Object valueToObject(Value value) {
        return bytesToObject(value.get());
    }

    private byte[] objectToBytes(Object obj) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(obj);
            oos.close();
            return baos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Object bytesToObject(byte[] bytes) {
        try {
            ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bytes));
            try {
                return ois.readObject();
            } finally {
                ois.close();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }
}
