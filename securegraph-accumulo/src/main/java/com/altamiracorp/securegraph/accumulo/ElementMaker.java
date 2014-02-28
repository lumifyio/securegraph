package com.altamiracorp.securegraph.accumulo;

import com.altamiracorp.securegraph.Property;
import com.altamiracorp.securegraph.SecureGraphException;
import com.altamiracorp.securegraph.Visibility;
import com.altamiracorp.securegraph.property.MutableProperty;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.RowDeletingIterator;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;

import java.util.*;

public abstract class ElementMaker<T> {
    private final Iterator<Map.Entry<Key, Value>> row;
    private final Map<String, String> propertyNames = new HashMap<String, String>();
    private final Map<String, Object> propertyValues = new HashMap<String, Object>();
    private final Map<String, Visibility> propertyVisibilities = new HashMap<String, Visibility>();
    private final Map<String, Map<String, Object>> propertyMetadata = new HashMap<String, Map<String, Object>>();
    private final AccumuloGraph graph;
    private String id;
    private Visibility visibility;

    public ElementMaker(AccumuloGraph graph, Iterator<Map.Entry<Key, Value>> row) {
        this.graph = graph;
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
            ColumnVisibility columnVisibility = getGraph().visibilityToAccumuloVisibility(col.getKey().getColumnVisibility().toString());
            Value value = col.getValue();

            if (columnFamily.equals(AccumuloGraph.DELETE_ROW_COLUMN_FAMILY)
                    && columnQualifier.equals(AccumuloGraph.DELETE_ROW_COLUMN_QUALIFIER)
                    && value.equals(RowDeletingIterator.DELETE_ROW_VALUE)) {
                return null;
            }

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

        // If the com.altamiracorp.securegraph.accumulo.iterator.ElementVisibilityRowFilter isn't installed this will catch stray rows
        if (this.visibility == null) {
            return null;
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

    public AccumuloGraph getGraph() {
        return graph;
    }

    protected List<Property> getProperties() {
        List<Property> results = new ArrayList<Property>(propertyValues.size());
        for (Map.Entry<String, Object> propertyValueEntry : propertyValues.entrySet()) {
            String propertyNameAndId = propertyValueEntry.getKey();
            String propertyId = getPropertyIdFromColumnQualifier(propertyNameAndId);
            String propertyName = propertyNames.get(propertyNameAndId);
            Object propertyValue = propertyValueEntry.getValue();
            Visibility visibility = propertyVisibilities.get(propertyNameAndId);
            Map<String, Object> metadata = propertyMetadata.get(propertyNameAndId);
            results.add(new MutableProperty(propertyId, propertyName, propertyValue, metadata, visibility));
        }
        return results;
    }

    private void extractPropertyMetadata(Text columnQualifier, Value value) {
        Object o;
        if (value.getSize() == 0) {
            o = new HashMap<String, Object>();
        } else {
            o = valueToObject(value);
        }
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
        Object v = valueToObject(value);
        String propertyName = getPropertyNameFromColumnQualifier(columnQualifier.toString());
        propertyNames.put(columnQualifier.toString(), propertyName);
        propertyValues.put(columnQualifier.toString(), v);
        propertyVisibilities.put(columnQualifier.toString(), accumuloVisibilityToVisibility(columnVisibility));
    }

    private String getPropertyNameFromColumnQualifier(String columnQualifier) {
        int i = columnQualifier.indexOf(ElementMutationBuilder.VALUE_SEPARATOR);
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
        int i = columnQualifier.indexOf(ElementMutationBuilder.VALUE_SEPARATOR);
        if (i < 0) {
            throw new SecureGraphException("Invalid property column qualifier");
        }
        return columnQualifier.substring(i + 1);
    }

    private Object valueToObject(Value value) {
        Object o = getGraph().getValueSerializer().valueToObject(value);
        if (o instanceof StreamingPropertyValueRef) {
            return ((StreamingPropertyValueRef) o).toStreamingPropertyValue(getGraph());
        }
        return o;
    }
}
