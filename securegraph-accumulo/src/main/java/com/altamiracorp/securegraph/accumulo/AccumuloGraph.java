package com.altamiracorp.securegraph.accumulo;

import com.altamiracorp.securegraph.*;
import com.altamiracorp.securegraph.accumulo.iterator.ElementVisibilityRowFilter;
import com.altamiracorp.securegraph.accumulo.serializer.ValueSerializer;
import com.altamiracorp.securegraph.id.IdGenerator;
import com.altamiracorp.securegraph.search.SearchIndex;
import com.altamiracorp.securegraph.util.LookAheadIterable;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class AccumuloGraph extends GraphBase {
    private static final Logger LOGGER = LoggerFactory.getLogger(AccumuloGraph.class);

    private static final Text EMPTY_TEXT = new Text("");
    private static final Value EMPTY_VALUE = new Value(new byte[0]);
    public static final String PROPERTY_ID_NAME_SEPERATOR = "\u001f";
    private final Connector connector;
    private final ValueSerializer valueSerializer;
    private BatchWriter writer;
    private final Object writerLock = new Object();

    protected AccumuloGraph(AccumuloGraphConfiguration config, IdGenerator idGenerator, SearchIndex searchIndex, Connector connector, ValueSerializer valueSerializer) {
        super(config, idGenerator, searchIndex);
        this.connector = connector;
        this.valueSerializer = valueSerializer;
    }

    public static AccumuloGraph create(AccumuloGraphConfiguration config) throws AccumuloSecurityException, AccumuloException, SecureGraphException {
        if (config == null) {
            throw new IllegalArgumentException("config cannot be null");
        }
        Connector connector = config.createConnector();
        ValueSerializer valueSerializer = config.createValueSerializer();
        SearchIndex searchIndex = config.createSearchIndex();
        IdGenerator idGenerator = config.createIdGenerator();
        return new AccumuloGraph(config, idGenerator, searchIndex, connector, valueSerializer);
    }

    public static AccumuloGraph create(Map config) throws AccumuloSecurityException, AccumuloException, SecureGraphException {
        return create(new AccumuloGraphConfiguration(config));
    }

    @Override
    public Vertex addVertex(Object vertexId, Visibility vertexVisibility, Property... properties) {
        ensureIdsOnProperties(properties);
        if (vertexId == null) {
            vertexId = getIdGenerator().nextId();
        }

        AccumuloVertex vertex = new AccumuloVertex(this, vertexId, vertexVisibility, properties);

        Mutation m = new Mutation(AccumuloVertex.ROW_KEY_PREFIX + vertex.getId());
        m.put(AccumuloVertex.CF_SIGNAL, EMPTY_TEXT, new ColumnVisibility(vertexVisibility.getVisibilityString()), EMPTY_VALUE);
        for (Property property : vertex.getProperties()) {
            addPropertyToMutation(m, property);
        }
        addMutations(m);

        getSearchIndex().addElement(this, vertex);

        return vertex;
    }

    void saveProperties(AccumuloElement element, Property[] properties) {
        ensureIdsOnProperties(properties);

        String rowPrefix = getRowPrefixForElement(element);

        Mutation m = new Mutation(rowPrefix + element.getId());
        for (Property property : properties) {
            addPropertyToMutation(m, property);
        }
        addMutations(m);

        getSearchIndex().addElement(this, element);
    }

    public void removeProperty(AccumuloElement element, Property property) {
        String rowPrefix = getRowPrefixForElement(element);

        Mutation m = new Mutation(rowPrefix + element.getId());
        addPropertyRemoveToMutation(m, property);
        addMutations(m);

        getSearchIndex().addElement(this, element);
    }

    private String getRowPrefixForElement(AccumuloElement element) {
        if (element instanceof Vertex) {
            return AccumuloVertex.ROW_KEY_PREFIX;
        }
        if (element instanceof Edge) {
            return AccumuloEdge.ROW_KEY_PREFIX;
        }
        throw new SecureGraphException("Unexpected element type: " + element.getClass().getName());
    }

    private void addPropertyToMutation(Mutation m, Property property) {
        Text columnQualifier = new Text(property.getName() + PROPERTY_ID_NAME_SEPERATOR + property.getId());
        ColumnVisibility columnVisibility = new ColumnVisibility(property.getVisibility().getVisibilityString());
        Value value = new Value(getValueSerializer().objectToValue(property.getValue()));
        m.put(AccumuloElement.CF_PROPERTY, columnQualifier, columnVisibility, value);
        Value metadataValue = new Value(getValueSerializer().objectToValue(property.getMetadata()));
        m.put(AccumuloElement.CF_PROPERTY_METADATA, columnQualifier, columnVisibility, metadataValue);
    }

    private void addPropertyRemoveToMutation(Mutation m, Property property) {
        Text columnQualifier = new Text(property.getName() + PROPERTY_ID_NAME_SEPERATOR + property.getId());
        ColumnVisibility columnVisibility = new ColumnVisibility(property.getVisibility().getVisibilityString());
        m.putDelete(AccumuloElement.CF_PROPERTY, columnQualifier, columnVisibility);
        m.putDelete(AccumuloElement.CF_PROPERTY_METADATA, columnQualifier, columnVisibility);
    }

    private void addMutations(Collection<Mutation> mutations) {
        try {
            BatchWriter writer = getWriter();
            synchronized (this.writerLock) {
                for (Mutation m : mutations) {
                    writer.addMutation(m);
                }
                if (getConfiguration().isAutoFlush()) {
                    writer.flush();
                }
            }
        } catch (MutationsRejectedException ex) {
            throw new RuntimeException("Could not add mutation", ex);
        }
    }

    // TODO consolodate this with addMutations(Collection) somehow
    private void addMutations(Mutation... mutations) {
        try {
            BatchWriter writer = getWriter();
            synchronized (this.writerLock) {
                for (Mutation m : mutations) {
                    writer.addMutation(m);
                }
                if (getConfiguration().isAutoFlush()) {
                    writer.flush();
                }
            }
        } catch (MutationsRejectedException ex) {
            throw new RuntimeException("Could not add mutation", ex);
        }
    }

    protected synchronized BatchWriter getWriter() {
        try {
            if (this.writer != null) {
                return this.writer;
            }
            BatchWriterConfig writerConfig = new BatchWriterConfig();
            this.writer = this.connector.createBatchWriter(getConfiguration().getTableName(), writerConfig);
            return this.writer;
        } catch (TableNotFoundException ex) {
            throw new RuntimeException("Could not create batch writer", ex);
        }
    }

    @Override
    public Iterable<Vertex> getVertices(Authorizations authorizations) throws SecureGraphException {
        return getVerticesInRange(null, null, authorizations);
    }

    @Override
    public void removeVertex(Vertex vertex, Authorizations authorizations) {
        if (vertex == null) {
            throw new IllegalArgumentException("vertex cannot be null");
        }

        List<Mutation> mutations = new ArrayList<Mutation>();

        getSearchIndex().removeElement(this, vertex);

        // Remove all edges that this vertex participates.
        for (Edge edge : vertex.getEdges(Direction.BOTH, authorizations)) {
            removeEdge(mutations, edge, authorizations);
        }

        addDeleteRowMutations(mutations, AccumuloVertex.ROW_KEY_PREFIX + vertex.getId(), authorizations);

        addMutations(mutations);
    }

    @Override
    public Edge addEdge(Object edgeId, Vertex outVertex, Vertex inVertex, String label, Visibility edgeVisibility, Property... properties) {
        if (outVertex == null) {
            throw new IllegalArgumentException("outVertex is required");
        }
        if (inVertex == null) {
            throw new IllegalArgumentException("inVertex is required");
        }
        if (edgeId == null) {
            edgeId = getIdGenerator().nextId();
        }

        ensureIdsOnProperties(properties);
        AccumuloEdge edge = new AccumuloEdge(this, edgeId, outVertex.getId(), inVertex.getId(), label, edgeVisibility, properties);

        Mutation addEdgeMutation = new Mutation(AccumuloEdge.ROW_KEY_PREFIX + edge.getId());
        ColumnVisibility edgeColumnVisibility = new ColumnVisibility(edgeVisibility.getVisibilityString());
        addEdgeMutation.put(AccumuloEdge.CF_SIGNAL, new Text(label), edgeColumnVisibility, EMPTY_VALUE);
        addEdgeMutation.put(AccumuloEdge.CF_OUT_VERTEX, new Text(outVertex.getId().toString()), edgeColumnVisibility, EMPTY_VALUE);
        addEdgeMutation.put(AccumuloEdge.CF_IN_VERTEX, new Text(inVertex.getId().toString()), edgeColumnVisibility, EMPTY_VALUE);
        for (Property property : edge.getProperties()) {
            addPropertyToMutation(addEdgeMutation, property);
        }

        // Update out vertex.
        Mutation addEdgeToOutMutation = new Mutation(AccumuloVertex.ROW_KEY_PREFIX + outVertex.getId());
        addEdgeToOutMutation.put(AccumuloVertex.CF_OUT_EDGE, new Text(edge.getId().toString()), edgeColumnVisibility, new Value(label.getBytes()));

        // Update in vertex.
        Mutation addEdgeToInMutation = new Mutation(AccumuloVertex.ROW_KEY_PREFIX + inVertex.getId());
        addEdgeToInMutation.put(AccumuloVertex.CF_IN_EDGE, new Text(edge.getId().toString()), edgeColumnVisibility, new Value(label.getBytes()));

        addMutations(addEdgeMutation, addEdgeToOutMutation, addEdgeToInMutation);

        if (outVertex instanceof AccumuloVertex) {
            ((AccumuloVertex) outVertex).addOutEdge(edge);
        }
        if (inVertex instanceof AccumuloVertex) {
            ((AccumuloVertex) inVertex).addInEdge(edge);
        }

        getSearchIndex().addElement(this, edge);

        return edge;
    }

    @Override
    public Iterable<Edge> getEdges(Authorizations authorizations) {
        return getEdgesInRange(null, null, authorizations);
    }

    @Override
    public void removeEdge(Edge edge, Authorizations authorizations) {
        List<Mutation> mutations = new ArrayList<Mutation>();
        removeEdge(mutations, edge, authorizations);
        addMutations(mutations);
    }

    @Override
    public void shutdown() {
        try {
            if (this.writer != null) {
                this.writer.flush();
            }
        } catch (Exception ex) {
            throw new SecureGraphException(ex);
        }
    }

    private void removeEdge(List<Mutation> mutations, Edge edge, Authorizations authorizations) {
        if (edge == null) {
            throw new IllegalArgumentException("edge cannot be null");
        }

        getSearchIndex().removeElement(this, edge);

        // Remove edge info from out/in vertices.
        // These may be null due to self loops, so need to check.
        Vertex out = edge.getVertex(Direction.OUT, authorizations);
        if (out != null) {
            Mutation m = new Mutation(AccumuloVertex.ROW_KEY_PREFIX + out.getId());
            m.putDelete(AccumuloVertex.CF_OUT_EDGE, new Text(edge.getId().toString()));
            mutations.add(m);
        }

        Vertex in = edge.getVertex(Direction.IN, authorizations);
        if (in != null) {
            Mutation m = new Mutation(AccumuloVertex.ROW_KEY_PREFIX + in.getId());
            m.putDelete(AccumuloVertex.CF_IN_EDGE, new Text(edge.getId().toString()));
            mutations.add(m);
        }

        // Remove everything else related to edge.
        addDeleteRowMutations(mutations, AccumuloEdge.ROW_KEY_PREFIX + edge.getId(), authorizations);
    }

    private void addDeleteRowMutations(List<Mutation> mutations, String rowKey, Authorizations authorizations) {
        // TODO: How do we delete rows if a user can't see all properties?
        try {
            Scanner scanner = connector.createScanner(getConfiguration().getTableName(), toAccumuloAuthorizations(authorizations));
            scanner.setRange(new Range(rowKey));
            Mutation m = new Mutation(rowKey);
            for (Map.Entry<Key, Value> col : scanner) {
                m.putDelete(col.getKey().getColumnFamily(), col.getKey().getColumnQualifier(), new ColumnVisibility(col.getKey().getColumnVisibility()));
            }
            mutations.add(m);
        } catch (TableNotFoundException ex) {
            throw new SecureGraphException(ex);
        }
    }

    public ValueSerializer getValueSerializer() {
        return valueSerializer;
    }

    @Override
    public AccumuloGraphConfiguration getConfiguration() {
        return (AccumuloGraphConfiguration) super.getConfiguration();
    }

    @Override
    public Vertex getVertex(Object vertexId, Authorizations authorizations) throws SecureGraphException {
        Iterator<Vertex> vertices = getVerticesInRange(vertexId, vertexId, authorizations).iterator();
        if (vertices.hasNext()) {
            return vertices.next();
        }
        return null;
    }

    private Iterable<Vertex> getVerticesInRange(Object startId, Object endId, Authorizations authorizations) throws SecureGraphException {
        final Scanner scanner = createVertexScanner(authorizations);

        Key startKey;
        if (startId == null) {
            startKey = new Key(AccumuloVertex.ROW_KEY_PREFIX);
        } else {
            startKey = new Key(AccumuloVertex.ROW_KEY_PREFIX + startId);
        }

        Key endKey;
        if (endId == null) {
            endKey = new Key(AccumuloVertex.AFTER_ROW_KEY_PREFIX);
        } else {
            endKey = new Key(AccumuloVertex.ROW_KEY_PREFIX + endId + "~");
        }

        scanner.setRange(new Range(startKey, endKey));
        scanner.clearColumns();

        return new LookAheadIterable<Iterator<Map.Entry<Key, Value>>, Vertex>() {

            @Override
            protected boolean isIncluded(Iterator<Map.Entry<Key, Value>> src, Vertex dest) {
                return dest != null;
            }

            @Override
            protected Vertex convert(Iterator<Map.Entry<Key, Value>> next) {
                return createVertexFromRow(next);
            }

            @Override
            protected Iterator<Iterator<Map.Entry<Key, Value>>> createIterator() {
                return new RowIterator(scanner.iterator());
            }
        };
    }

    private Property[] toProperties(Map<String, String> propertyNames, Map<String, Object> propertyValues, Map<String, Visibility> propertyVisibilities, Map<String, Map<String, Object>> propertyMetadata) {
        Property[] results = new Property[propertyValues.size()];
        int i = 0;
        for (Map.Entry<String, Object> propertyValueEntry : propertyValues.entrySet()) {
            String propertyNameAndId = propertyValueEntry.getKey();
            Object propertyId = getPropertyIdFromColumnQualifier(propertyNameAndId);
            String propertyName = propertyNames.get(propertyNameAndId);
            Object propertyValue = propertyValueEntry.getValue();
            Visibility visibility = propertyVisibilities.get(propertyNameAndId);
            Map<String, Object> metadata = propertyMetadata.get(propertyNameAndId);
            results[i++] = new Property(propertyId, propertyName, propertyValue, visibility, metadata);
        }
        return results;
    }

    private String vertexIdFromRowKey(String rowKey) throws SecureGraphException {
        if (rowKey.startsWith(AccumuloVertex.ROW_KEY_PREFIX)) {
            return rowKey.substring(AccumuloVertex.ROW_KEY_PREFIX.length());
        }
        throw new SecureGraphException("Invalid row key for vertex: " + rowKey);
    }

    private String edgeIdFromRowKey(String rowKey) throws SecureGraphException {
        if (rowKey.startsWith(AccumuloEdge.ROW_KEY_PREFIX)) {
            return rowKey.substring(AccumuloEdge.ROW_KEY_PREFIX.length());
        }
        throw new SecureGraphException("Invalid row key for edge: " + rowKey);
    }

    private Scanner createVertexScanner(Authorizations authorizations) throws SecureGraphException {
        return createElementVisibilityScanner(authorizations, ElementVisibilityRowFilter.OPT_FILTER_VERTICES);
    }

    private Scanner createEdgeScanner(Authorizations authorizations) throws SecureGraphException {
        return createElementVisibilityScanner(authorizations, ElementVisibilityRowFilter.OPT_FILTER_EDGES);
    }

    private Scanner createElementVisibilityScanner(Authorizations authorizations, String elementMode) throws SecureGraphException {
        try {
            Scanner scanner = connector.createScanner(getConfiguration().getTableName(), toAccumuloAuthorizations(authorizations));
            IteratorSetting iteratorSetting = new IteratorSetting(
                    100,
                    ElementVisibilityRowFilter.class.getSimpleName(),
                    ElementVisibilityRowFilter.class
            );
            iteratorSetting.addOption(elementMode, Boolean.TRUE.toString());
            scanner.addScanIterator(iteratorSetting);
            return scanner;
        } catch (TableNotFoundException e) {
            throw new SecureGraphException(e);
        }
    }

    private org.apache.accumulo.core.security.Authorizations toAccumuloAuthorizations(Authorizations authorizations) {
        return new org.apache.accumulo.core.security.Authorizations(authorizations.getAuthorizations());
    }

    @Override
    public Edge getEdge(Object edgeId, Authorizations authorizations) {
        Iterator<Edge> edges = getEdgesInRange(edgeId, edgeId, authorizations).iterator();
        if (edges.hasNext()) {
            return edges.next();
        }
        return null;
    }

    private Iterable<Edge> getEdgesInRange(Object startId, Object endId, Authorizations authorizations) throws SecureGraphException {
        final Scanner scanner = createEdgeScanner(authorizations);

        Key startKey;
        if (startId == null) {
            startKey = new Key(AccumuloEdge.ROW_KEY_PREFIX);
        } else {
            startKey = new Key(AccumuloEdge.ROW_KEY_PREFIX + startId);
        }

        Key endKey;
        if (endId == null) {
            endKey = new Key(AccumuloEdge.AFTER_ROW_KEY_PREFIX);
        } else {
            endKey = new Key(AccumuloEdge.ROW_KEY_PREFIX + endId + "~");
        }

        scanner.setRange(new Range(startKey, endKey));
        scanner.clearColumns();

        return new LookAheadIterable<Iterator<Map.Entry<Key, Value>>, Edge>() {

            @Override
            protected boolean isIncluded(Iterator<Map.Entry<Key, Value>> src, Edge dest) {
                return dest != null;
            }

            @Override
            protected Edge convert(Iterator<Map.Entry<Key, Value>> next) {
                return createEdgeFromRow(next);
            }

            @Override
            protected Iterator<Iterator<Map.Entry<Key, Value>>> createIterator() {
                return new RowIterator(scanner.iterator());
            }
        };
    }

    private Vertex createVertexFromRow(Iterator<Map.Entry<Key, Value>> row) throws SecureGraphException {
        String id = null;
        Map<String, String> propertyNames = new HashMap<String, String>();
        Map<String, Object> propertyValues = new HashMap<String, Object>();
        Map<String, Visibility> propertyVisibilities = new HashMap<String, Visibility>();
        Map<String, Map<String, Object>> propertyMetadata = new HashMap<String, Map<String, Object>>();
        HashSet<Object> inEdgeIds = new HashSet<Object>();
        HashSet<Object> outEdgeIds = new HashSet<Object>();
        Visibility vertexVisibility = null;

        while (row.hasNext()) {
            Map.Entry<Key, Value> col = row.next();
            if (id == null) {
                id = vertexIdFromRowKey(col.getKey().getRow().toString());
            }
            Text columnFamily = col.getKey().getColumnFamily();
            Text columnQualifier = col.getKey().getColumnQualifier();
            ColumnVisibility columnVisibility = new ColumnVisibility(col.getKey().getColumnVisibility().toString());
            Value value = col.getValue();

            if (AccumuloElement.CF_PROPERTY.toString().equals(columnFamily.toString())) {
                Object v = getValueSerializer().valueToObject(value);
                String propertyName = getPropertyNameFromColumnQualifier(columnQualifier.toString());
                propertyNames.put(columnQualifier.toString(), propertyName);
                propertyValues.put(columnQualifier.toString(), v);
                propertyVisibilities.put(columnQualifier.toString(), accumuloVisibilityToVisibility(columnVisibility));
                continue;
            }

            if (AccumuloElement.CF_PROPERTY_METADATA.toString().equals(columnFamily.toString())) {
                Object o = getValueSerializer().valueToObject(value);
                if (o == null) {
                    throw new SecureGraphException("Invalid metadata found. Expected " + Map.class.getName() + ". Found null.");
                } else if (o instanceof Map) {
                    Map v = (Map) o;
                    propertyMetadata.put(columnQualifier.toString(), v);
                } else {
                    throw new SecureGraphException("Invalid metadata found. Expected " + Map.class.getName() + ". Found " + o.getClass().getName() + ".");
                }
                continue;
            }

            if (AccumuloVertex.CF_SIGNAL.toString().equals(columnFamily.toString())) {
                vertexVisibility = accumuloVisibilityToVisibility(columnVisibility);
                continue;
            }

            if (AccumuloVertex.CF_OUT_EDGE.toString().equals(columnFamily.toString())) {
                outEdgeIds.add(columnQualifier.toString());
                continue;
            }

            if (AccumuloVertex.CF_IN_EDGE.toString().equals(columnFamily.toString())) {
                inEdgeIds.add(columnQualifier.toString());
                continue;
            }

            LOGGER.debug("Unhandled column {} {}", columnFamily, columnQualifier);
        }

        if (vertexVisibility == null) {
            throw new SecureGraphException("Invalid vertex visibility. This could occur if other columns are returned without the vertex signal column being returned.");
        }
        Property[] properties = toProperties(propertyNames, propertyValues, propertyVisibilities, propertyMetadata);
        return new AccumuloVertex(this, id, vertexVisibility, properties, inEdgeIds, outEdgeIds);
    }

    private Object getPropertyIdFromColumnQualifier(String columnQualifier) {
        int i = columnQualifier.indexOf(PROPERTY_ID_NAME_SEPERATOR);
        if (i < 0) {
            throw new SecureGraphException("Invalid property column qualifier");
        }
        return columnQualifier.substring(i + 1);
    }

    private String getPropertyNameFromColumnQualifier(String columnQualifier) {
        int i = columnQualifier.indexOf(PROPERTY_ID_NAME_SEPERATOR);
        if (i < 0) {
            throw new SecureGraphException("Invalid property column qualifier");
        }
        return columnQualifier.substring(0, i);
    }

    private Edge createEdgeFromRow(Iterator<Map.Entry<Key, Value>> row) {
        String id = null;
        Map<String, String> propertyNames = new HashMap<String, String>();
        Map<String, Object> propertyValues = new HashMap<String, Object>();
        Map<String, Visibility> propertyVisibilities = new HashMap<String, Visibility>();
        Map<String, Map<String, Object>> propertyMetadata = new HashMap<String, Map<String, Object>>();
        String outVertexId = null;
        String inVertexId = null;
        String label = null;
        Visibility edgeVisibility = null;

        while (row.hasNext()) {
            Map.Entry<Key, Value> col = row.next();
            if (id == null) {
                id = edgeIdFromRowKey(col.getKey().getRow().toString());
            }
            Text columnFamily = col.getKey().getColumnFamily();
            Text columnQualifier = col.getKey().getColumnQualifier();
            ColumnVisibility columnVisibility = new ColumnVisibility(col.getKey().getColumnVisibility().toString());
            Value value = col.getValue();

            // TODO this is duplicate logic from createVertexFromRow
            if (AccumuloElement.CF_PROPERTY.toString().equals(columnFamily.toString())) {
                Object v = getValueSerializer().valueToObject(value);
                String propertyName = getPropertyNameFromColumnQualifier(columnQualifier.toString());
                propertyNames.put(columnQualifier.toString(), propertyName);
                propertyValues.put(columnQualifier.toString(), v);
                propertyVisibilities.put(columnQualifier.toString(), accumuloVisibilityToVisibility(columnVisibility));
                continue;
            }

            // TODO this is duplicate logic from createVertexFromRow
            if (AccumuloElement.CF_PROPERTY_METADATA.toString().equals(columnFamily.toString())) {
                Object o = getValueSerializer().valueToObject(value);
                if (o == null) {
                    throw new SecureGraphException("Invalid metadata found. Expected " + Map.class.getName() + ". Found null.");
                } else if (o instanceof Map) {
                    Map v = (Map) o;
                    propertyMetadata.put(columnQualifier.toString(), v);
                } else {
                    throw new SecureGraphException("Invalid metadata found. Expected " + Map.class.getName() + ". Found " + o.getClass().getName() + ".");
                }
                continue;
            }

            if (AccumuloEdge.CF_SIGNAL.toString().equals(columnFamily.toString())) {
                edgeVisibility = accumuloVisibilityToVisibility(columnVisibility);
                label = columnQualifier.toString();
                continue;
            }

            if (AccumuloEdge.CF_IN_VERTEX.toString().equals(columnFamily.toString())) {
                inVertexId = columnQualifier.toString();
                continue;
            }

            if (AccumuloEdge.CF_OUT_VERTEX.toString().equals(columnFamily.toString())) {
                outVertexId = columnQualifier.toString();
                continue;
            }

            LOGGER.debug("Unhandled column {} {}", columnFamily, columnQualifier);
        }

        if (edgeVisibility == null) {
            throw new SecureGraphException("Invalid vertex visibility. This could occur if other columns are returned without the vertex signal column being returned.");
        }
        Property[] properties = toProperties(propertyNames, propertyValues, propertyVisibilities, propertyMetadata);
        return new AccumuloEdge(this, id, outVertexId, inVertexId, label, edgeVisibility, properties);
    }

    private Visibility accumuloVisibilityToVisibility(ColumnVisibility columnVisibility) {
        String columnVisibilityString = columnVisibility.toString();
        if (columnVisibilityString.startsWith("[") && columnVisibilityString.endsWith("]")) {
            return new Visibility(columnVisibilityString.substring(1, columnVisibilityString.length() - 1));
        }
        return new Visibility(columnVisibilityString);
    }

    private void printTable(Authorizations authorizations) {
        try {
            Scanner scanner = connector.createScanner(getConfiguration().getTableName(), toAccumuloAuthorizations(authorizations));
            RowIterator it = new RowIterator(scanner.iterator());
            while (it.hasNext()) {
                boolean first = true;
                Text lastColumnFamily = null;
                Iterator<Map.Entry<Key, Value>> row = it.next();
                while (row.hasNext()) {
                    Map.Entry<Key, Value> col = row.next();
                    if (first) {
                        System.out.println("\"" + col.getKey().getRow() + "\"");
                        first = false;
                    }
                    if (!col.getKey().getColumnFamily().equals(lastColumnFamily)) {
                        System.out.println("  \"" + col.getKey().getColumnFamily() + "\"");
                        lastColumnFamily = col.getKey().getColumnFamily();
                    }
                    System.out.println("    \"" + col.getKey().getColumnQualifier() + "\"(" + col.getKey().getColumnVisibility() + ")=\"" + col.getValue() + "\"");
                }
            }
            System.out.flush();
        } catch (TableNotFoundException e) {
            throw new SecureGraphException(e);
        }
    }
}
