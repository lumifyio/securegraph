package org.securegraph.accumulo.migrations;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.securegraph.accumulo.ElementMutationBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

public class MetadataToRowsMR extends MRMigrationBase {
    private static final Logger LOGGER = LoggerFactory.getLogger(MetadataToRowsMR.class);

    public static void main(String[] args) throws Exception {
        run(new MetadataToRowsMR(), args);
    }

    @Override
    protected Class<? extends Mapper> getMigrationMapperClass() {
        return MetadataToRowsMRMapper.class;
    }

    public static class MetadataToRowsMRMapper extends MRMigrationMapperBase {
        @Override
        protected void safeMap(Key key, Value value, Context context) throws IOException, InterruptedException {
            context.setStatus(key.getRow().toString());

            if (!key.getColumnFamily().toString().equals("PROPMETA")) {
                return;
            }

            if (value.getSize() == 0) {
                return;
            }

            Object propMetadataMapObj = getValueSerializer().valueToObject(value);
            if (!(propMetadataMapObj instanceof Map)) {
                return;
            }

            Mutation m = new Mutation(key.getRow());
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("mutation: " + key.getRow());
            }

            Map<String, Object> propMetadataMap = (Map<String, Object>) propMetadataMapObj;
            for (Map.Entry<String, Object> mapEntry : propMetadataMap.entrySet()) {
                Text newColumnQualifier = new Text(key.getColumnQualifier().toString() + ElementMutationBuilder.VALUE_SEPARATOR + mapEntry.getKey());
                Value newValue = getValueSerializer().objectToValue(mapEntry.getValue());
                m.put(key.getColumnFamily(), newColumnQualifier, key.getColumnVisibilityParsed(), key.getTimestamp(), newValue);
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("  put: " + key.getColumnFamily() + ", " + newColumnQualifier + ", " + key.getColumnVisibilityParsed() + ", " + key.getTimestamp() + ", " + newValue);
                }
            }

            m.putDelete(key.getColumnFamily(), key.getColumnQualifier(), key.getColumnVisibilityParsed(), key.getTimestamp());
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("  put delete: " + key.getColumnFamily() + ", " + key.getColumnQualifier() + ", " + key.getColumnVisibilityParsed() + ", " + key.getTimestamp());
            }
            context.write(getOutputTableNameText(), m);
        }

        private int count(String columnQualifier, String valueSeparator) {
            int fromIndex = 0;
            int count = 0;
            while (true) {
                fromIndex = columnQualifier.indexOf(valueSeparator, fromIndex);
                if (fromIndex < 0) {
                    break;
                }
                fromIndex++;
                count++;
            }
            return count;
        }
    }
}
