package org.securegraph.sql.model;

import org.securegraph.GraphMetadataEntry;
import org.securegraph.sql.serializer.ValueSerializer;
import org.securegraph.util.ConvertingIterable;

import java.util.List;

public class SqlMetadataModel {
    private String key;
    private byte[] value;

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public byte[] getValue() {
        return value;
    }

    public void setValue(byte[] value) {
        this.value = value;
    }

    private Object getValueObject(ValueSerializer valueSerializer) {
        return valueSerializer.toObject(getValue());
    }

    public static Iterable<GraphMetadataEntry> toGraphMetadataEntry(final ValueSerializer valueSerializer, List<SqlMetadataModel> metadatas) {
        return new ConvertingIterable<SqlMetadataModel, GraphMetadataEntry>(metadatas) {
            @Override
            protected GraphMetadataEntry convert(SqlMetadataModel metadata) {
                return metadata.toGraphMetadataEntry(valueSerializer);
            }
        };
    }

    private GraphMetadataEntry toGraphMetadataEntry(ValueSerializer valueSerializer) {
        return new GraphMetadataEntry(getKey(), getValueObject(valueSerializer));
    }
}
