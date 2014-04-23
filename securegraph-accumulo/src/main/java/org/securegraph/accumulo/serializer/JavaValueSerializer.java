package org.securegraph.accumulo.serializer;

import org.securegraph.util.JavaSerializableUtils;
import org.apache.accumulo.core.data.Value;

import java.util.Map;

public class JavaValueSerializer implements ValueSerializer {
    public JavaValueSerializer(Map configuration) {

    }

    @Override
    public Value objectToValue(Object value) {
        return new Value(JavaSerializableUtils.objectToBytes(value));
    }

    @Override
    public <T> T valueToObject(Value value) {
        return (T) JavaSerializableUtils.bytesToObject(value.get());
    }
}
