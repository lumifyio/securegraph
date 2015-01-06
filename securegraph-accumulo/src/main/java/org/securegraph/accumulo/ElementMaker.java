package org.securegraph.accumulo;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.RowDeletingIterator;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.securegraph.Authorizations;
import org.securegraph.Property;
import org.securegraph.SecureGraphException;
import org.securegraph.Visibility;

import java.util.*;

public abstract class ElementMaker<T> {
    private final Iterator<Map.Entry<Key, Value>> row;
    private final Map<String, String> propertyNames = new HashMap<String, String>();
    private final Map<String, String> propertyColumnQualifier = new HashMap<String, String>();
    private final Map<String, byte[]> propertyValues = new HashMap<String, byte[]>();
    private final Map<String, Visibility> propertyVisibilities = new HashMap<String, Visibility>();
    private final Map<String, Map<String, byte[]>> propertyMetadata = new HashMap<String, Map<String, byte[]>>();
    private final Map<String, Map<String, Visibility>> propertyMetadataVisibilities = new HashMap<String, Map<String, Visibility>>();
    private final Set<HiddenProperty> hiddenProperties = new HashSet<HiddenProperty>();
    private final Set<Visibility> hiddenVisibilities = new HashSet<Visibility>();
    private final AccumuloGraph graph;
    private final Authorizations authorizations;
    private String id;
    private Visibility visibility;

    public ElementMaker(AccumuloGraph graph, Iterator<Map.Entry<Key, Value>> row, Authorizations authorizations) {
        this.graph = graph;
        this.row = row;
        this.authorizations = authorizations;
    }

    public T make(boolean includeHidden) {
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

            if (columnFamily.equals(AccumuloElement.CF_HIDDEN)) {
                if (includeHidden) {
                    this.hiddenVisibilities.add(accumuloVisibilityToVisibility(columnVisibility));
                } else {
                    return null;
                }
            }

            if (columnFamily.equals(AccumuloElement.CF_PROPERTY_HIDDEN)) {
                extractPropertyHidden(columnQualifier, columnVisibility);
            }

            if (AccumuloElement.CF_PROPERTY.compareTo(columnFamily) == 0) {
                extractPropertyData(columnQualifier, columnVisibility, value);
                continue;
            }

            if (AccumuloElement.CF_PROPERTY_METADATA.compareTo(columnFamily) == 0) {
                extractPropertyMetadata(columnQualifier, columnVisibility, value);
                continue;
            }

            if (getVisibilitySignal().equals(columnFamily.toString())) {
                this.visibility = accumuloVisibilityToVisibility(columnVisibility);
            }

            processColumn(col.getKey(), col.getValue());
        }

        // If the org.securegraph.accumulo.iterator.ElementVisibilityRowFilter isn't installed this will catch stray rows
        if (this.visibility == null) {
            return null;
        }

        return makeElement(includeHidden);
    }

    protected abstract void processColumn(Key key, Value value);

    protected abstract String getIdFromRowKey(String rowKey);

    protected abstract String getVisibilitySignal();

    protected abstract T makeElement(boolean includeHidden);

    protected String getId() {
        return this.id;
    }

    protected Visibility getVisibility() {
        return this.visibility;
    }

    public AccumuloGraph getGraph() {
        return graph;
    }

    protected Set<Visibility> getHiddenVisibilities() {
        return hiddenVisibilities;
    }

    protected List<Property> getProperties(boolean includeHidden) {
        List<Property> results = new ArrayList<Property>(propertyValues.size());
        for (Map.Entry<String, byte[]> propertyValueEntry : propertyValues.entrySet()) {
            String key = propertyValueEntry.getKey();
            String propertyKey = getPropertyKeyFromColumnQualifier(propertyColumnQualifier.get(key));
            String propertyName = propertyNames.get(key);
            byte[] propertyValue = propertyValueEntry.getValue();
            Visibility propertyVisibility = propertyVisibilities.get(key);
            Set<Visibility> propertyHiddenVisibilities = getPropertyHiddenVisibilities(propertyKey, propertyName, propertyVisibility);
            if (!includeHidden && isHidden(propertyKey, propertyName, propertyVisibility)) {
                continue;
            }
            Map<String, byte[]> metadata = propertyMetadata.get(key);
            Map<String, Visibility> metadataVisibilities = propertyMetadataVisibilities.get(key);
            results.add(new LazyMutableProperty(getGraph(), getGraph().getValueSerializer(), propertyKey, propertyName, propertyValue, metadata, metadataVisibilities, propertyHiddenVisibilities, propertyVisibility));
        }
        return results;
    }

    private Set<Visibility> getPropertyHiddenVisibilities(String propertyKey, String propertyName, Visibility propertyVisibility) {
        Set<Visibility> hiddenVisibilities = null;
        for (HiddenProperty hiddenProperty : hiddenProperties) {
            if (hiddenProperty.matches(propertyKey, propertyName, propertyVisibility)) {
                if (hiddenVisibilities == null) {
                    hiddenVisibilities = new HashSet<Visibility>();
                }
                hiddenVisibilities.add(hiddenProperty.getHiddenVisibility());
            }
        }
        return hiddenVisibilities;
    }

    private boolean isHidden(String propertyKey, String propertyName, Visibility visibility) {
        for (HiddenProperty hiddenProperty : hiddenProperties) {
            if (hiddenProperty.matches(propertyKey, propertyName, visibility)) {
                return true;
            }
        }
        return false;
    }

    private void extractPropertyHidden(Text columnQualifier, ColumnVisibility columnVisibility) {
        String columnQualifierStr = columnQualifier.toString();
        int nameKeySep = columnQualifierStr.indexOf(ElementMutationBuilder.VALUE_SEPARATOR);
        if (nameKeySep < 0) {
            throw new SecureGraphException("Invalid property hidden column qualifier");
        }
        int keyVisSep = columnQualifierStr.indexOf(ElementMutationBuilder.VALUE_SEPARATOR, nameKeySep + 1);
        if (nameKeySep < 0) {
            throw new SecureGraphException("Invalid property hidden column qualifier");
        }

        String name = columnQualifierStr.substring(0, nameKeySep);
        String key = columnQualifierStr.substring(nameKeySep + 1, keyVisSep);
        String vis = columnQualifierStr.substring(keyVisSep + 1);

        this.hiddenProperties.add(new HiddenProperty(key, name, vis, accumuloVisibilityToVisibility(columnVisibility)));
    }

    private void extractPropertyMetadata(Text columnQualifier, ColumnVisibility columnVisibility, Value value) {
        PropertyMetadataColumnQualifier propertyMetadataColumnQualifier = new PropertyMetadataColumnQualifier(columnQualifier, columnVisibility);

        Map<String, byte[]> values = propertyMetadata.get(propertyMetadataColumnQualifier.getPropertyKey());
        if (values == null) {
            values = new HashMap<String, byte[]>();
            propertyMetadata.put(propertyMetadataColumnQualifier.getPropertyKey(), values);
        }
        values.put(propertyMetadataColumnQualifier.getMetadataKey(), value.get());

        Map<String, Visibility> visibilities = propertyMetadataVisibilities.get(propertyMetadataColumnQualifier.getPropertyKey());
        if (visibilities == null) {
            visibilities = new HashMap<String, Visibility>();
            propertyMetadataVisibilities.put(propertyMetadataColumnQualifier.getPropertyKey(), visibilities);
        }
        visibilities.put(propertyMetadataColumnQualifier.getMetadataKey(), accumuloVisibilityToVisibility(columnVisibility));
    }

    private void extractPropertyData(Text columnQualifier, ColumnVisibility columnVisibility, Value value) {
        String propertyName = getPropertyNameFromColumnQualifier(columnQualifier.toString());
        String key = propertyColumnQualifierToKey(columnQualifier, columnVisibility);
        propertyColumnQualifier.put(key, columnQualifier.toString());
        propertyNames.put(key, propertyName);
        propertyValues.put(key, value.get());
        propertyVisibilities.put(key, accumuloVisibilityToVisibility(columnVisibility));
    }

    private String propertyColumnQualifierToKey(Text columnQualifier, ColumnVisibility columnVisibility) {
        return columnQualifier.toString() + ":" + columnVisibility.toString();
    }

    private static class PropertyMetadataColumnQualifier {
        private final String propertyKey;
        private final String metadataKey;

        public PropertyMetadataColumnQualifier(Text columnQualifier, ColumnVisibility columnVisibility) {
            String columnQualifierString = columnQualifier.toString();
            int i = columnQualifierString.lastIndexOf(ElementMutationBuilder.VALUE_SEPARATOR);
            if (i < 0) {
                throw new SecureGraphException("Invalid property metadata column qualifier: " + columnQualifierString);
            }
            String propertyKeyPart = columnQualifierString.substring(0, i);
            metadataKey = columnQualifierString.substring(i + 1);
            propertyKey = propertyKeyPart + ":" + columnVisibility.toString();
        }

        public String getPropertyKey() {
            return propertyKey;
        }

        public String getMetadataKey() {
            return metadataKey;
        }
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
        return accumuloVisibilityToVisibility(columnVisibilityString);
    }

    private Visibility accumuloVisibilityToVisibility(String columnVisibilityString) {
        if (columnVisibilityString.startsWith("[") && columnVisibilityString.endsWith("]")) {
            return new Visibility(columnVisibilityString.substring(1, columnVisibilityString.length() - 1));
        }
        return new Visibility(columnVisibilityString);
    }

    private String getPropertyKeyFromColumnQualifier(String columnQualifier) {
        int i = columnQualifier.indexOf(ElementMutationBuilder.VALUE_SEPARATOR);
        if (i < 0) {
            throw new SecureGraphException("Invalid property column qualifier");
        }
        return columnQualifier.substring(i + 1);
    }

    public Authorizations getAuthorizations() {
        return authorizations;
    }

    private static class HiddenProperty {
        private final String key;
        private final String name;
        private final String visibility;
        private final Visibility hiddenVisibility;

        public HiddenProperty(String key, String name, String visibility, Visibility hiddenVisibility) {
            this.key = key;
            this.name = name;
            this.visibility = visibility;
            this.hiddenVisibility = hiddenVisibility;
        }

        public boolean matches(String propertyKey, String propertyName, Visibility visibility) {
            return propertyKey.equals(this.key)
                    && propertyName.equals(this.name)
                    && visibility.getVisibilityString().equals(this.visibility);
        }

        public Visibility getHiddenVisibility() {
            return hiddenVisibility;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            HiddenProperty that = (HiddenProperty) o;

            if (key != null ? !key.equals(that.key) : that.key != null) {
                return false;
            }
            if (name != null ? !name.equals(that.name) : that.name != null) {
                return false;
            }
            if (visibility != null ? !visibility.equals(that.visibility) : that.visibility != null) {
                return false;
            }

            return true;
        }

        @Override
        public int hashCode() {
            int result = key != null ? key.hashCode() : 0;
            result = 31 * result + (name != null ? name.hashCode() : 0);
            result = 31 * result + (visibility != null ? visibility.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            return "HiddenProperty{" +
                    "key='" + key + '\'' +
                    ", name='" + name + '\'' +
                    ", visibility='" + visibility + '\'' +
                    '}';
        }
    }
}
