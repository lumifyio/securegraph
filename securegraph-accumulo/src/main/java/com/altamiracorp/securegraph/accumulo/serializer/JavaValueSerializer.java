package com.altamiracorp.securegraph.accumulo.serializer;

import com.altamiracorp.securegraph.util.JavaSerializableUtils;
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
    public Object valueToObject(Value value) {
        return JavaSerializableUtils.bytesToObject(value.get());
    }
}
