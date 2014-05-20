package org.securegraph.accumulo.serializer.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.accumulo.core.data.Value;
import org.securegraph.accumulo.EdgeInfo;
import org.securegraph.accumulo.serializer.ValueSerializer;
import org.securegraph.type.GeoPoint;

import java.util.HashMap;
import java.util.Map;

public class KryoValueSerializer implements ValueSerializer {
    private final Kryo kryo;

    public KryoValueSerializer(Map configuration) {
        kryo = new Kryo();
        kryo.register(EdgeInfo.class, 1);
        kryo.register(GeoPoint.class, 2);
        kryo.register(HashMap.class, 3);
    }

    @Override
    public Value objectToValue(Object value) {
        Output output = new Output();
        kryo.writeClassAndObject(output, value);
        return new Value(output.toBytes());
    }

    @Override
    public <T> T valueToObject(Value value) {
        Input input = new Input(value.get());
        return (T) kryo.readClassAndObject(input);
    }
}
