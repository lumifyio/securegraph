package org.securegraph.accumulo;

import org.securegraph.SecureGraphException;
import org.securegraph.Visibility;
import org.securegraph.accumulo.serializer.ValueSerializer;
import org.securegraph.property.MutableProperty;

import java.util.HashMap;
import java.util.Map;

public class LazyMutableProperty extends MutableProperty {
    private final AccumuloGraph graph;
    private final ValueSerializer valueSerializer;
    private final String propertyKey;
    private final String propertyName;
    private byte[] propertyValue;
    private final byte[] metadata;
    private Visibility visibility;
    private transient Object cachedPropertyValue;
    private transient Map<String, Object> cachedMetadata;

    public LazyMutableProperty(AccumuloGraph graph, ValueSerializer valueSerializer, String propertyKey, String propertyName, byte[] propertyValue, byte[] metadata, Visibility visibility) {
        this.graph = graph;
        this.valueSerializer = valueSerializer;
        this.propertyKey = propertyKey;
        this.propertyName = propertyName;
        this.propertyValue = propertyValue;
        this.metadata = metadata;
        this.visibility = visibility;
    }

    @Override
    public void setValue(Object value) {
        this.cachedPropertyValue = value;
        this.propertyValue = null;
    }

    @Override
    public void setVisibility(Visibility visibility) {
        this.visibility = visibility;
    }

    @Override
    public String getKey() {
        return this.propertyKey;
    }

    @Override
    public String getName() {
        return this.propertyName;
    }

    @Override
    public Object getValue() {
        if (cachedPropertyValue == null) {
            if (propertyValue == null || propertyValue.length == 0) {
                return null;
            }
            cachedPropertyValue = this.valueSerializer.valueToObject(propertyValue);
            if (cachedPropertyValue instanceof StreamingPropertyValueRef) {
                cachedPropertyValue = ((StreamingPropertyValueRef) cachedPropertyValue).toStreamingPropertyValue(this.graph);
            }
        }
        return cachedPropertyValue;
    }

    @Override
    public Visibility getVisibility() {
        return this.visibility;
    }

    @Override
    public Map<String, Object> getMetadata() {
        if (cachedMetadata == null) {
            if (metadata.length == 0) {
                cachedMetadata = new HashMap<String, Object>();
            } else {
                Object o = this.valueSerializer.valueToObject(metadata);
                if (o == null) {
                    throw new SecureGraphException("Invalid metadata found. Expected " + Map.class.getName() + ". Found null.");
                } else if (o instanceof Map) {
                    cachedMetadata = (Map) o;
                } else {
                    throw new SecureGraphException("Invalid metadata found. Expected " + Map.class.getName() + ". Found " + o.getClass().getName() + ".");
                }
            }
        }
        return cachedMetadata;
    }
}
