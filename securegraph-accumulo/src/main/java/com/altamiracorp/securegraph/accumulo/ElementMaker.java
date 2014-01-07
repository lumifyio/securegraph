package com.altamiracorp.securegraph.accumulo;

import com.altamiracorp.securegraph.Property;
import com.altamiracorp.securegraph.SecureGraphException;
import com.altamiracorp.securegraph.Visibility;
import com.altamiracorp.securegraph.accumulo.serializer.ValueSerializer;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public abstract class ElementMaker<T> {
    private final ValueSerializer valueSerializer;
    private final Iterator<Map.Entry<Key, Value>> row;
    private final Map<String, String> propertyNames = new HashMap<String, String>();
    private final Map<String, Object> propertyValues = new HashMap<String, Object>();
    private final Map<String, Visibility> propertyVisibilities = new HashMap<String, Visibility>();
    private final Map<String, Map<String, Object>> propertyMetadata = new HashMap<String, Map<String, Object>>();
    private String id;
    private Visibility visibility;

    public ElementMaker(ValueSerializer valueSerializer, Iterator<Map.Entry<Key, Value>> row) {
        this.valueSerializer = valueSerializer;
        this.row = row;
    }

    public T make() {
        while (row.hasNext()) {
            Map.Entry<Key, Value> col = row.next();

            if (this.id == null) {
                this.id = getIdFromRowKey(col.getKey().getRow().toString());
            }

            Text columnFamily = col.getKey().getColumnFamily();
            Text columnQualifier = col.getKey().getColumnQualifier();
            ColumnVisibility columnVisibility = new ColumnVisibility(col.getKey().getColumnVisibility().toString());
            Value value = col.getValue();

            if (AccumuloElement.CF_PROPERTY.compareTo(columnFamily) == 0) {
                extractPropertyData(columnQualifier, columnVisibility, value);
                continue;
            }

            if (AccumuloElement.CF_PROPERTY_METADATA.compareTo(columnFamily) == 0) {
                extractPropertyMetadata(columnQualifier, value);
                continue;
            }

            if (getVisibilitySignal().equals(columnFamily.toString())) {
                this.visibility = accumuloVisibilityToVisibility(columnVisibility);
            }

            processColumn(col.getKey(), col.getValue());
        }

        if (this.visibility == null) {
            throw new SecureGraphException("Invalid visibility. This could occur if other columns are returned without the element signal column being returned.");
        }

        return makeElement();
    }

    protected abstract void processColumn(Key key, Value value);

    protected abstract String getIdFromRowKey(String rowKey);

    protected abstract String getVisibilitySignal();

    protected abstract T makeElement();

    protected String getId() {
        return this.id;
    }

    protected Visibility getVisibility() {
        return this.visibility;
    }

    protected Property[] getProperties() {
        Property[] results = new Property[propertyValues.size()];
        int i = 0;
        for (Map.Entry<String, Object> propertyValueEntry : propertyValues.entrySet()) {
            String propertyNameAndId = propertyValueEntry.getKey();
            String propertyId = getPropertyIdFromColumnQualifier(propertyNameAndId);
            String propertyName = propertyNames.get(propertyNameAndId);
            Object propertyValue = propertyValueEntry.getValue();
            Visibility visibility = propertyVisibilities.get(propertyNameAndId);
            Map<String, Object> metadata = propertyMetadata.get(propertyNameAndId);
            results[i++] = new Property(propertyId, propertyName, propertyValue, visibility, metadata);
        }
        return results;
    }

    private void extractPropertyMetadata(Text columnQualifier, Value value) {
        Object o = this.valueSerializer.valueToObject(value);
        if (o == null) {
            throw new SecureGraphException("Invalid metadata found. Expected " + Map.class.getName() + ". Found null.");
        } else if (o instanceof Map) {
            Map v = (Map) o;
            propertyMetadata.put(columnQualifier.toString(), v);
        } else {
            throw new SecureGraphException("Invalid metadata found. Expected " + Map.class.getName() + ". Found " + o.getClass().getName() + ".");
        }
    }

    private void extractPropertyData(Text columnQualifier, ColumnVisibility columnVisibility, Value value) {
        Object v = this.valueSerializer.valueToObject(value);
        String propertyName = getPropertyNameFromColumnQualifier(columnQualifier.toString());
        propertyNames.put(columnQualifier.toString(), propertyName);
        propertyValues.put(columnQualifier.toString(), v);
        propertyVisibilities.put(columnQualifier.toString(), accumuloVisibilityToVisibility(columnVisibility));
    }

    private String getPropertyNameFromColumnQualifier(String columnQualifier) {
        int i = columnQualifier.indexOf(AccumuloGraph.PROPERTY_ID_NAME_SEPERATOR);
        if (i < 0) {
            throw new SecureGraphException("Invalid property column qualifier");
        }
        return columnQualifier.substring(0, i);
    }

    private Visibility accumuloVisibilityToVisibility(ColumnVisibility columnVisibility) {
        String columnVisibilityString = columnVisibility.toString();
        if (columnVisibilityString.startsWith("[") && columnVisibilityString.endsWith("]")) {
            return new Visibility(columnVisibilityString.substring(1, columnVisibilityString.length() - 1));
        }
        return new Visibility(columnVisibilityString);
    }

    private String getPropertyIdFromColumnQualifier(String columnQualifier) {
        int i = columnQualifier.indexOf(AccumuloGraph.PROPERTY_ID_NAME_SEPERATOR);
        if (i < 0) {
            throw new SecureGraphException("Invalid property column qualifier");
        }
        return columnQualifier.substring(i + 1);
    }
}
