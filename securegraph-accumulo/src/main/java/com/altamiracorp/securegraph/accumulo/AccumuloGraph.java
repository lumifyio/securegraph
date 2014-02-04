package com.altamiracorp.securegraph.accumulo;

import com.altamiracorp.securegraph.*;
import com.altamiracorp.securegraph.accumulo.iterator.ElementVisibilityRowFilter;
import com.altamiracorp.securegraph.accumulo.serializer.ValueSerializer;
import com.altamiracorp.securegraph.id.IdGenerator;
import com.altamiracorp.securegraph.property.MutableProperty;
import com.altamiracorp.securegraph.property.StreamingPropertyValue;
import com.altamiracorp.securegraph.search.SearchIndex;
import com.altamiracorp.securegraph.util.LimitOutputStream;
import com.altamiracorp.securegraph.util.LookAheadIterable;
import com.altamiracorp.securegraph.util.StreamUtils;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.RowDeletingIterator;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.*;

import static com.altamiracorp.securegraph.util.IterableUtils.toList;
import static com.altamiracorp.securegraph.util.Preconditions.checkNotNull;

public class AccumuloGraph extends GraphBase {
    private static final Text EMPTY_TEXT = new Text("");
    private static final Value EMPTY_VALUE = new Value(new byte[0]);
    public static final String VALUE_SEPARATOR = "\u001f";
    private static final String ROW_DELETING_ITERATOR_NAME = RowDeletingIterator.class.getSimpleName();
    private static final int ROW_DELETING_ITERATOR_PRIORITY = 7;
    public static final Text DELETE_ROW_COLUMN_FAMILY = new Text("");
    public static final Text DELETE_ROW_COLUMN_QUALIFIER = new Text("");
    public static final String VERTEX_AFTER_ROW_KEY_PREFIX = "W";
    public static final String EDGE_AFTER_ROW_KEY_PREFIX = "F";
    private final Connector connector;
    private final ValueSerializer valueSerializer;
    private BatchWriter verticesWriter;
    private BatchWriter edgesWriter;
    private BatchWriter dataWriter;
    private final Object writerLock = new Object();
    private long maxStreamingPropertyValueTableDataSize;
    private final FileSystem fileSystem;
    private String dataDir;

    protected AccumuloGraph(AccumuloGraphConfiguration config, IdGenerator idGenerator, SearchIndex searchIndex, Connector connector, FileSystem fileSystem, ValueSerializer valueSerializer) {
        super(config, idGenerator, searchIndex);
        this.connector = connector;
        this.fileSystem = fileSystem;
        this.valueSerializer = valueSerializer;
        this.maxStreamingPropertyValueTableDataSize = config.getMaxStreamingPropertyValueTableDataSize();
        this.dataDir = config.getDataDir();
    }

    public static AccumuloGraph create(AccumuloGraphConfiguration config) throws AccumuloSecurityException, AccumuloException, SecureGraphException, InterruptedException, IOException, URISyntaxException {
        if (config == null) {
            throw new IllegalArgumentException("config cannot be null");
        }
        Connector connector = config.createConnector();
        FileSystem fs = config.createFileSystem();
        ValueSerializer valueSerializer = config.createValueSerializer();
        SearchIndex searchIndex = config.createSearchIndex();
        IdGenerator idGenerator = config.createIdGenerator();
        ensureTableExists(connector, getVerticesTableName(config.getTableNamePrefix()));
        ensureTableExists(connector, getEdgesTableName(config.getTableNamePrefix()));
        ensureTableExists(connector, getDataTableName(config.getTableNamePrefix()));
        ensureRowDeletingIteratorIsAttached(connector, getVerticesTableName(config.getTableNamePrefix()));
        ensureRowDeletingIteratorIsAttached(connector, getEdgesTableName(config.getTableNamePrefix()));
        ensureRowDeletingIteratorIsAttached(connector, getDataTableName(config.getTableNamePrefix()));
        return new AccumuloGraph(config, idGenerator, searchIndex, connector, fs, valueSerializer);
    }

    private static void ensureTableExists(Connector connector, String tableName) {
        try {
            if (!connector.tableOperations().exists(tableName)) {
                connector.tableOperations().create(tableName);
            }
        } catch (Exception e) {
            throw new RuntimeException("Unable to create table " + tableName);
        }
    }

    private static void ensureRowDeletingIteratorIsAttached(Connector connector, String tableName) {
        try {
            IteratorSetting is = new IteratorSetting(ROW_DELETING_ITERATOR_PRIORITY, ROW_DELETING_ITERATOR_NAME, RowDeletingIterator.class);
            if (!connector.tableOperations().listIterators(tableName).containsKey(ROW_DELETING_ITERATOR_NAME)) {
                connector.tableOperations().attachIterator(tableName, is);
            }
        } catch (Exception e) {
            throw new SecureGraphException("Could not attach RowDeletingIterator", e);
        }
    }

    public static AccumuloGraph create(Map config) throws AccumuloSecurityException, AccumuloException, SecureGraphException, InterruptedException, IOException, URISyntaxException {
        return create(new AccumuloGraphConfiguration(config));
    }

    @Override
    public Vertex addVertex(Object vertexId, Visibility vertexVisibility, Authorizations authorizations) {
        return prepareVertex(vertexId, vertexVisibility, authorizations).save();
    }

    @Override
    public VertexBuilder prepareVertex(Object vertexId, Visibility visibility, Authorizations authorizations) {
        if (vertexId == null) {
            vertexId = getIdGenerator().nextId();
        }

        return new VertexBuilder(vertexId, visibility) {
            @Override
            public Vertex save() {
                AccumuloVertex vertex = new AccumuloVertex(AccumuloGraph.this, getVertexId(), getVisibility(), getProperties());

                String vertexRowKey = AccumuloConstants.VERTEX_ROW_KEY_PREFIX + vertex.getId();
                Mutation m = new Mutation(vertexRowKey);
                m.put(AccumuloVertex.CF_SIGNAL, EMPTY_TEXT, new ColumnVisibility(getVisibility().getVisibilityString()), EMPTY_VALUE);
                for (Property property : vertex.getProperties()) {
                    addPropertyToMutation(m, vertexRowKey, property);
                }
                addMutations(getVerticesWriter(), m);

                getSearchIndex().addElement(AccumuloGraph.this, vertex);

                return vertex;
            }
        };
    }

    void saveProperties(AccumuloElement element, Iterable<Property> properties) {
        String rowPrefix = getRowPrefixForElement(element);

        String elementRowKey = rowPrefix + element.getId();
        Mutation m = new Mutation(elementRowKey);
        boolean hasProperty = false;
        for (Property property : properties) {
            hasProperty = true;
            addPropertyToMutation(m, elementRowKey, property);
        }
        if (hasProperty) {
            addMutations(getWriterFromElementType(element), m);

            getSearchIndex().addElement(this, element);
        }
    }

    void removeProperty(AccumuloElement element, Property property) {
        String rowPrefix = getRowPrefixForElement(element);

        Mutation m = new Mutation(rowPrefix + element.getId());
        addPropertyRemoveToMutation(m, property);
        addMutations(getWriterFromElementType(element), m);

        getSearchIndex().addElement(this, element);
    }

    private String getRowPrefixForElement(AccumuloElement element) {
        if (element instanceof Vertex) {
            return AccumuloConstants.VERTEX_ROW_KEY_PREFIX;
        }
        if (element instanceof Edge) {
            return AccumuloConstants.EDGE_ROW_KEY_PREFIX;
        }
        throw new SecureGraphException("Unexpected element type: " + element.getClass().getName());
    }

    private void addPropertyToMutation(Mutation m, String rowKey, Property property) {
        Text columnQualifier = new Text(property.getName() + VALUE_SEPARATOR + property.getKey());
        ColumnVisibility columnVisibility = new ColumnVisibility(property.getVisibility().getVisibilityString());
        Object propertyValue = property.getValue();
        if (propertyValue instanceof StreamingPropertyValue) {
            StreamingPropertyValueRef streamingPropertyValueRef = saveStreamingPropertyValue(rowKey, property, (StreamingPropertyValue) propertyValue);
            ((MutableProperty) property).setValue(streamingPropertyValueRef.toStreamingPropertyValue(this));
            propertyValue = streamingPropertyValueRef;
        }
        Value value = new Value(getValueSerializer().objectToValue(propertyValue));
        m.put(AccumuloElement.CF_PROPERTY, columnQualifier, columnVisibility, value);
        if (property.getMetadata() != null && property.getMetadata().size() > 0) {
            Value metadataValue = new Value(getValueSerializer().objectToValue(property.getMetadata()));
            m.put(AccumuloElement.CF_PROPERTY_METADATA, columnQualifier, columnVisibility, metadataValue);
        } else {
            m.put(AccumuloElement.CF_PROPERTY_METADATA, columnQualifier, columnVisibility, EMPTY_VALUE);
        }
    }

    private StreamingPropertyValueRef saveStreamingPropertyValue(String rowKey, Property property, StreamingPropertyValue propertyValue) {
        try {
            HdfsLargeDataStore largeDataStore = new HdfsLargeDataStore(this.fileSystem);
            LimitOutputStream out = new LimitOutputStream(largeDataStore, maxStreamingPropertyValueTableDataSize);
            try {
                StreamUtils.copy(propertyValue.getInputStream(), out);
            } finally {
                out.close();
            }

            if (out.hasExceededSizeLimit()) {
                return saveStreamingPropertyValueLarge(rowKey, property, largeDataStore, propertyValue);
            } else {
                return saveStreamingPropertyValueSmall(rowKey, property, out.getSmall(), propertyValue);
            }
        } catch (IOException ex) {
            throw new SecureGraphException(ex);
        }
    }

    private StreamingPropertyValueRef saveStreamingPropertyValueLarge(String rowKey, Property property, HdfsLargeDataStore largeDataStore, StreamingPropertyValue propertyValue) throws IOException {
        Path dir = new Path(dataDir, rowKey);
        fileSystem.mkdirs(dir);
        Path path = new Path(dir, property.getName() + "_" + property.getKey());
        if (fileSystem.exists(path)) {
            fileSystem.delete(path, true);
        }
        fileSystem.rename(largeDataStore.getFileName(), path);
        return new StreamingPropertyValueHdfsRef(path, propertyValue);
    }

    private StreamingPropertyValueRef saveStreamingPropertyValueSmall(String rowKey, Property property, byte[] data, StreamingPropertyValue propertyValue) {
        String dataRowKey = createTableDataRowKey(rowKey, property);
        Mutation dataMutation = new Mutation(dataRowKey);
        dataMutation.put(EMPTY_TEXT, EMPTY_TEXT, new Value(data));
        addMutations(getDataWriter(), dataMutation);
        return new StreamingPropertyValueTableRef(dataRowKey, propertyValue, data);
    }

    private String createTableDataRowKey(String rowKey, Property property) {
        return AccumuloConstants.DATA_ROW_KEY_PREFIX + rowKey + VALUE_SEPARATOR + property.getName() + VALUE_SEPARATOR + property.getKey();
    }

    private void addPropertyRemoveToMutation(Mutation m, Property property) {
        Text columnQualifier = new Text(property.getName() + VALUE_SEPARATOR + property.getKey());
        ColumnVisibility columnVisibility = new ColumnVisibility(property.getVisibility().getVisibilityString());
        m.putDelete(AccumuloElement.CF_PROPERTY, columnQualifier, columnVisibility);
        m.putDelete(AccumuloElement.CF_PROPERTY_METADATA, columnQualifier, columnVisibility);
    }

    private void addMutations(BatchWriter writer, Mutation... mutations) {
        try {
            synchronized (this.writerLock) {
                for (Mutation m : mutations) {
                    writer.addMutation(m);
                }
                if (getConfiguration().isAutoFlush()) {
                    flush();
                }
            }
        } catch (MutationsRejectedException ex) {
            throw new RuntimeException("Could not add mutation", ex);
        }
    }

    protected synchronized BatchWriter getVerticesWriter() {
        try {
            if (this.verticesWriter != null) {
                return this.verticesWriter;
            }
            BatchWriterConfig writerConfig = new BatchWriterConfig();
            this.verticesWriter = this.connector.createBatchWriter(getVerticesTableName(), writerConfig);
            return this.verticesWriter;
        } catch (TableNotFoundException ex) {
            throw new RuntimeException("Could not create batch writer", ex);
        }
    }

    protected synchronized BatchWriter getEdgesWriter() {
        try {
            if (this.edgesWriter != null) {
                return this.edgesWriter;
            }
            BatchWriterConfig writerConfig = new BatchWriterConfig();
            this.edgesWriter = this.connector.createBatchWriter(getEdgesTableName(), writerConfig);
            return this.edgesWriter;
        } catch (TableNotFoundException ex) {
            throw new RuntimeException("Could not create batch writer", ex);
        }
    }

    protected BatchWriter getWriterFromElementType(Element element) {
        if (element instanceof Vertex) {
            return getVerticesWriter();
        } else if (element instanceof Edge) {
            return getEdgesWriter();
        } else {
            throw new SecureGraphException("Unexpected element type: " + element.getClass().getName());
        }
    }

    protected synchronized BatchWriter getDataWriter() {
        try {
            if (this.dataWriter != null) {
                return this.dataWriter;
            }
            BatchWriterConfig writerConfig = new BatchWriterConfig();
            this.dataWriter = this.connector.createBatchWriter(getDataTableName(), writerConfig);
            return this.dataWriter;
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

        getSearchIndex().removeElement(this, vertex);

        // Remove all edges that this vertex participates.
        for (Edge edge : vertex.getEdges(Direction.BOTH, authorizations)) {
            removeEdge(edge, authorizations);
        }

        addMutations(getVerticesWriter(), getDeleteRowMutation(AccumuloConstants.VERTEX_ROW_KEY_PREFIX + vertex.getId()));
    }

    @Override
    public EdgeBuilder prepareEdge(Object edgeId, Vertex outVertex, Vertex inVertex, String label, Visibility visibility, Authorizations authorizations) {
        if (outVertex == null) {
            throw new IllegalArgumentException("outVertex is required");
        }
        if (inVertex == null) {
            throw new IllegalArgumentException("inVertex is required");
        }
        if (edgeId == null) {
            edgeId = getIdGenerator().nextId();
        }

        return new EdgeBuilder(edgeId, outVertex, inVertex, label, visibility) {
            @Override
            public Edge save() {
                AccumuloEdge edge = new AccumuloEdge(AccumuloGraph.this, getEdgeId(), getOutVertex().getId(), getInVertex().getId(), getLabel(), getVisibility(), getProperties());

                String edgeRowKey = AccumuloConstants.EDGE_ROW_KEY_PREFIX + edge.getId();
                Mutation addEdgeMutation = new Mutation(edgeRowKey);
                ColumnVisibility edgeColumnVisibility = new ColumnVisibility(getVisibility().getVisibilityString());
                addEdgeMutation.put(AccumuloEdge.CF_SIGNAL, new Text(getLabel()), edgeColumnVisibility, EMPTY_VALUE);
                addEdgeMutation.put(AccumuloEdge.CF_OUT_VERTEX, new Text(getOutVertex().getId().toString()), edgeColumnVisibility, EMPTY_VALUE);
                addEdgeMutation.put(AccumuloEdge.CF_IN_VERTEX, new Text(getInVertex().getId().toString()), edgeColumnVisibility, EMPTY_VALUE);
                for (Property property : edge.getProperties()) {
                    addPropertyToMutation(addEdgeMutation, edgeRowKey, property);
                }

                // Update out vertex.
                Mutation addEdgeToOutMutation = new Mutation(AccumuloConstants.VERTEX_ROW_KEY_PREFIX + getOutVertex().getId());
                EdgeInfo edgeInfo = new EdgeInfo(getLabel(), getInVertex().getId());
                addEdgeToOutMutation.put(AccumuloVertex.CF_OUT_EDGE, new Text(edge.getId().toString()), edgeColumnVisibility, getValueSerializer().objectToValue(edgeInfo));

                // Update in vertex.
                Mutation addEdgeToInMutation = new Mutation(AccumuloConstants.VERTEX_ROW_KEY_PREFIX + getInVertex().getId());
                edgeInfo = new EdgeInfo(getLabel(), getOutVertex().getId());
                addEdgeToInMutation.put(AccumuloVertex.CF_IN_EDGE, new Text(edge.getId().toString()), edgeColumnVisibility, getValueSerializer().objectToValue(edgeInfo));

                addMutations(getEdgesWriter(), addEdgeMutation);
                addMutations(getVerticesWriter(), addEdgeToOutMutation, addEdgeToInMutation);

                if (getOutVertex() instanceof AccumuloVertex) {
                    ((AccumuloVertex) getOutVertex()).addOutEdge(edge);
                }
                if (getInVertex() instanceof AccumuloVertex) {
                    ((AccumuloVertex) getInVertex()).addInEdge(edge);
                }

                getSearchIndex().addElement(AccumuloGraph.this, edge);

                return edge;
            }
        };
    }

    @Override
    public Edge addEdge(Object edgeId, Vertex outVertex, Vertex inVertex, String label, Visibility edgeVisibility, Authorizations authorizations) {
        return prepareEdge(edgeId, outVertex, inVertex, label, edgeVisibility, authorizations).save();
    }

    @Override
    public Iterable<Edge> getEdges(Authorizations authorizations) {
        return getEdgesInRange(null, null, authorizations);
    }

    @Override
    public Iterable<Object> findRelatedEdges(Iterable<Object> vertexIds, Authorizations authorizations) {
        Set<Object> results = new HashSet<Object>();
        List<Vertex> vertices = toList(getVertices(vertexIds, authorizations));

        // since we are checking bi-directional edges we should only have to check v1->v2 and not v2->v1
        Map<String, String> checkedCombinations = new HashMap<String, String>();

        for (Vertex sourceVertex : vertices) {
            for (Vertex destVertex : vertices) {
                if (checkedCombinations.containsKey(sourceVertex.getId().toString() + destVertex.getId().toString())) {
                    continue;
                }
                Iterable<Object> edgeIds = ((AccumuloVertex) sourceVertex).getEdgeIds(destVertex.getId(), Direction.BOTH, authorizations);
                for (Object edgeId : edgeIds) {
                    results.add(edgeId);
                }
                checkedCombinations.put(sourceVertex.getId().toString() + destVertex.getId().toString(), "");
                checkedCombinations.put(destVertex.getId().toString() + sourceVertex.getId().toString(), "");
            }
        }
        return results;
    }

    @Override
    public void removeEdge(Edge edge, Authorizations authorizations) {
        checkNotNull(edge);

        getSearchIndex().removeElement(this, edge);

        Vertex out = edge.getVertex(Direction.OUT, authorizations);
        checkNotNull(out, "Unable to delete edge %s, can't find out vertex", edge.getId());
        Vertex in = edge.getVertex(Direction.IN, authorizations);
        checkNotNull(in, "Unable to delete edge %s, can't find in vertex", edge.getId());

        ColumnVisibility visibility = visibilityToAccumuloVisibility(edge.getVisibility());

        Mutation outMutation = new Mutation(AccumuloConstants.VERTEX_ROW_KEY_PREFIX + out.getId());
        outMutation.putDelete(AccumuloVertex.CF_OUT_EDGE, new Text(edge.getId().toString()), visibility);

        Mutation inMutation = new Mutation(AccumuloConstants.VERTEX_ROW_KEY_PREFIX + in.getId());
        inMutation.putDelete(AccumuloVertex.CF_IN_EDGE, new Text(edge.getId().toString()), visibility);

        addMutations(getVerticesWriter(), outMutation, inMutation);

        // Remove everything else related to edge.
        addMutations(getEdgesWriter(), getDeleteRowMutation(AccumuloConstants.EDGE_ROW_KEY_PREFIX + edge.getId()));

        if (out instanceof AccumuloVertex) {
            ((AccumuloVertex) out).removeOutEdge(edge);
        }
        if (in instanceof AccumuloVertex) {
            ((AccumuloVertex) out).removeInEdge(edge);
        }
    }

    @Override
    public void flush() {
        flushWriter(this.dataWriter);
        flushWriter(this.verticesWriter);
        flushWriter(this.edgesWriter);
        getSearchIndex().flush();
    }

    private static void flushWriter(BatchWriter writer) {
        if (writer != null) {
            try {
                writer.flush();
            } catch (MutationsRejectedException e) {
                throw new SecureGraphException("Could not flush", e);
            }
        }
    }

    @Override
    public void shutdown() {
        try {
            flush();
            if (this.dataWriter != null) {
                this.dataWriter.close();
                this.dataWriter = null;
            }
            if (this.verticesWriter != null) {
                this.verticesWriter.close();
                this.verticesWriter = null;
            }
            if (this.edgesWriter != null) {
                this.edgesWriter.close();
                this.edgesWriter = null;
            }
        } catch (Exception ex) {
            throw new SecureGraphException(ex);
        }
    }

    private Mutation getDeleteRowMutation(String rowKey) {
        Mutation m = new Mutation(rowKey);
        m.put(DELETE_ROW_COLUMN_FAMILY, DELETE_ROW_COLUMN_QUALIFIER, RowDeletingIterator.DELETE_ROW_VALUE);
        return m;
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

    @Override
    public Iterable<Vertex> getVertices(Iterable<Object> ids, final Authorizations authorizations) {
        final AccumuloGraph graph = this;

        final List<Range> ranges = new ArrayList<Range>();
        for (Object id : ids) {
            Text rowKey = new Text(AccumuloConstants.VERTEX_ROW_KEY_PREFIX + id);
            ranges.add(new Range(rowKey));
        }
        if (ranges.size() == 0) {
            return new ArrayList<Vertex>();
        }

        return new LookAheadIterable<Map.Entry<Key, Value>, Vertex>() {
            public BatchScanner batchScanner;

            @Override
            protected boolean isIncluded(Map.Entry<Key, Value> src, Vertex dest) {
                return dest != null;
            }

            @Override
            protected Vertex convert(Map.Entry<Key, Value> wholeRow) {
                try {
                    SortedMap<Key, Value> row = WholeRowIterator.decodeRow(wholeRow.getKey(), wholeRow.getValue());
                    VertexMaker maker = new VertexMaker(graph, row.entrySet().iterator());
                    return maker.make();
                } catch (IOException ex) {
                    throw new SecureGraphException("Could not recreate row", ex);
                }
            }

            @Override
            protected Iterator<Map.Entry<Key, Value>> createIterator() {
                batchScanner = createVertexBatchScanner(authorizations, Math.min(Math.max(1, ranges.size() / 10), 10));
                batchScanner.setRanges(ranges);
                batchScanner.clearColumns();
                return batchScanner.iterator();
            }

            @Override
            protected void done() {
                super.done();
                batchScanner.close();
            }
        };
    }

    private Iterable<Vertex> getVerticesInRange(Object startId, Object endId, final Authorizations authorizations) throws SecureGraphException {
        final AccumuloGraph graph = this;

        final Key startKey;
        if (startId == null) {
            startKey = new Key(AccumuloConstants.VERTEX_ROW_KEY_PREFIX);
        } else {
            startKey = new Key(AccumuloConstants.VERTEX_ROW_KEY_PREFIX + startId);
        }

        final Key endKey;
        if (endId == null) {
            endKey = new Key(VERTEX_AFTER_ROW_KEY_PREFIX);
        } else {
            endKey = new Key(AccumuloConstants.VERTEX_ROW_KEY_PREFIX + endId + "~");
        }

        return new LookAheadIterable<Iterator<Map.Entry<Key, Value>>, Vertex>() {
            public Scanner scanner;

            @Override
            protected boolean isIncluded(Iterator<Map.Entry<Key, Value>> src, Vertex dest) {
                return dest != null;
            }

            @Override
            protected Vertex convert(Iterator<Map.Entry<Key, Value>> next) {
                VertexMaker maker = new VertexMaker(graph, next);
                return maker.make();
            }

            @Override
            protected Iterator<Iterator<Map.Entry<Key, Value>>> createIterator() {
                scanner = createVertexScanner(authorizations);
                scanner.setRange(new Range(startKey, endKey));
                scanner.clearColumns();
                return new RowIterator(scanner.iterator());
            }

            @Override
            protected void done() {
                super.done();
                scanner.close();
            }
        };
    }

    Scanner createVertexScanner(Authorizations authorizations) throws SecureGraphException {
        return createElementVisibilityScanner(authorizations, ElementType.VERTEX);
    }

    Scanner createEdgeScanner(Authorizations authorizations) throws SecureGraphException {
        return createElementVisibilityScanner(authorizations, ElementType.EDGE);
    }

    private Scanner createElementVisibilityScanner(Authorizations authorizations, ElementType elementType) throws SecureGraphException {
        try {
            String tableName = getTableNameFromElementType(elementType);
            Scanner scanner = connector.createScanner(tableName, toAccumuloAuthorizations(authorizations));
            if (getConfiguration().isUseServerSideElementVisibilityRowFilter()) {
                IteratorSetting iteratorSetting = new IteratorSetting(
                        100,
                        ElementVisibilityRowFilter.class.getSimpleName(),
                        ElementVisibilityRowFilter.class
                );
                String elementMode = getElementModeFromElementType(elementType);
                iteratorSetting.addOption(elementMode, Boolean.TRUE.toString());
                scanner.addScanIterator(iteratorSetting);
            }
            return scanner;
        } catch (TableNotFoundException e) {
            throw new SecureGraphException(e);
        }
    }

    private BatchScanner createVertexBatchScanner(Authorizations authorizations, int numQueryThreads) throws SecureGraphException {
        return createElementVisibilityBatchScanner(authorizations, ElementType.VERTEX, numQueryThreads);
    }

    private BatchScanner createEdgeBatchScanner(Authorizations authorizations, int numQueryThreads) throws SecureGraphException {
        return createElementVisibilityBatchScanner(authorizations, ElementType.EDGE, numQueryThreads);
    }

    private BatchScanner createElementVisibilityBatchScanner(Authorizations authorizations, ElementType elementType, int numQueryThreads) throws SecureGraphException {
        try {
            String tableName = getTableNameFromElementType(elementType);
            BatchScanner scanner = connector.createBatchScanner(tableName, toAccumuloAuthorizations(authorizations), numQueryThreads);
            IteratorSetting iteratorSetting;
            if (getConfiguration().isUseServerSideElementVisibilityRowFilter()) {
                iteratorSetting = new IteratorSetting(
                        100,
                        ElementVisibilityRowFilter.class.getSimpleName(),
                        ElementVisibilityRowFilter.class
                );
                String elementMode = getElementModeFromElementType(elementType);
                iteratorSetting.addOption(elementMode, Boolean.TRUE.toString());
                scanner.addScanIterator(iteratorSetting);
            }

            iteratorSetting = new IteratorSetting(
                    101,
                    WholeRowIterator.class.getSimpleName(),
                    WholeRowIterator.class
            );
            scanner.addScanIterator(iteratorSetting);

            return scanner;
        } catch (TableNotFoundException e) {
            throw new SecureGraphException(e);
        }
    }

    private String getTableNameFromElementType(ElementType elementType) {
        String tableName;
        switch (elementType) {
            case VERTEX:
                tableName = getVerticesTableName();
                break;
            case EDGE:
                tableName = getEdgesTableName();
                break;
            default:
                throw new SecureGraphException("Unexpected element type: " + elementType);
        }
        return tableName;
    }

    private String getElementModeFromElementType(ElementType elementType) {
        String elementMode;
        switch (elementType) {
            case VERTEX:
                elementMode = ElementVisibilityRowFilter.OPT_FILTER_VERTICES;
                break;
            case EDGE:
                elementMode = ElementVisibilityRowFilter.OPT_FILTER_EDGES;
                break;
            default:
                throw new SecureGraphException("Unexpected element type: " + elementType);
        }
        return elementMode;
    }

    private org.apache.accumulo.core.security.Authorizations toAccumuloAuthorizations(Authorizations authorizations) {
        if (authorizations == null) {
            throw new NullPointerException("authorizations is required");
        }
        return new org.apache.accumulo.core.security.Authorizations(((AccumuloAuthorizations) authorizations).getAuthorizations());
    }

    @Override
    public Edge getEdge(Object edgeId, Authorizations authorizations) {
        Iterator<Edge> edges = getEdgesInRange(edgeId, edgeId, authorizations).iterator();
        if (edges.hasNext()) {
            return edges.next();
        }
        return null;
    }

    @Override
    public Iterable<Edge> getEdges(Iterable<Object> ids, final Authorizations authorizations) {
        final AccumuloGraph graph = this;

        final List<Range> ranges = new ArrayList<Range>();
        for (Object id : ids) {
            Text rowKey = new Text(AccumuloConstants.EDGE_ROW_KEY_PREFIX + id);
            ranges.add(new Range(rowKey));
        }
        if (ranges.size() == 0) {
            return new ArrayList<Edge>();
        }

        return new LookAheadIterable<Map.Entry<Key, Value>, Edge>() {
            public BatchScanner batchScanner;

            @Override
            protected boolean isIncluded(Map.Entry<Key, Value> src, Edge dest) {
                return dest != null;
            }

            @Override
            protected Edge convert(Map.Entry<Key, Value> wholeRow) {
                try {
                    SortedMap<Key, Value> row = WholeRowIterator.decodeRow(wholeRow.getKey(), wholeRow.getValue());
                    EdgeMaker maker = new EdgeMaker(graph, row.entrySet().iterator());
                    return maker.make();
                } catch (IOException ex) {
                    throw new SecureGraphException("Could not recreate row", ex);
                }
            }

            @Override
            protected Iterator<Map.Entry<Key, Value>> createIterator() {
                batchScanner = createEdgeBatchScanner(authorizations, Math.min(Math.max(1, ranges.size() / 10), 10));
                batchScanner.setRanges(ranges);
                batchScanner.clearColumns();
                return batchScanner.iterator();
            }

            @Override
            protected void done() {
                super.done();
                batchScanner.close();
            }
        };
    }

    private Iterable<Edge> getEdgesInRange(Object startId, Object endId, final Authorizations authorizations) throws SecureGraphException {
        final AccumuloGraph graph = this;

        final Key startKey;
        if (startId == null) {
            startKey = new Key(AccumuloConstants.EDGE_ROW_KEY_PREFIX);
        } else {
            startKey = new Key(AccumuloConstants.EDGE_ROW_KEY_PREFIX + startId);
        }

        final Key endKey;
        if (endId == null) {
            endKey = new Key(EDGE_AFTER_ROW_KEY_PREFIX);
        } else {
            endKey = new Key(AccumuloConstants.EDGE_ROW_KEY_PREFIX + endId + "~");
        }

        return new LookAheadIterable<Iterator<Map.Entry<Key, Value>>, Edge>() {
            public Scanner scanner;

            @Override
            protected boolean isIncluded(Iterator<Map.Entry<Key, Value>> src, Edge dest) {
                return dest != null;
            }

            @Override
            protected Edge convert(Iterator<Map.Entry<Key, Value>> next) {
                EdgeMaker maker = new EdgeMaker(graph, next);
                return maker.make();
            }

            @Override
            protected Iterator<Iterator<Map.Entry<Key, Value>>> createIterator() {
                scanner = createEdgeScanner(authorizations);
                scanner.setRange(new Range(startKey, endKey));
                scanner.clearColumns();
                return new RowIterator(scanner.iterator());
            }

            @Override
            protected void done() {
                super.done();
                scanner.close();
            }
        };
    }

    private void printTable(Authorizations authorizations) {
        String[] tables = new String[]{getEdgesTableName(), getVerticesTableName(), getDataTableName()};
        System.out.println("---------------------------------------------- BEGIN printTable ----------------------------------------------");
        try {
            for (String tableName : tables) {
                System.out.println("TABLE: " + tableName);
                System.out.println("");
                Scanner scanner = connector.createScanner(tableName, toAccumuloAuthorizations(authorizations));
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
            }
            System.out.flush();
        } catch (TableNotFoundException e) {
            throw new SecureGraphException(e);
        }
        System.out.println("---------------------------------------------- END printTable ------------------------------------------------");
    }

    public byte[] streamingPropertyValueTableData(String dataRowKey) {
        try {
            Scanner scanner = connector.createScanner(getDataTableName(), new org.apache.accumulo.core.security.Authorizations());
            scanner.setRange(new Range(dataRowKey));
            Iterator<Map.Entry<Key, Value>> it = scanner.iterator();
            if (it.hasNext()) {
                Map.Entry<Key, Value> col = it.next();
                return col.getValue().get();
            }
        } catch (Exception ex) {
            throw new SecureGraphException(ex);
        }
        throw new SecureGraphException("Unexpected end of row: " + dataRowKey);
    }

    public FileSystem getFileSystem() {
        return fileSystem;
    }

    private ColumnVisibility visibilityToAccumuloVisibility(Visibility visibility) {
        return new ColumnVisibility(visibility.getVisibilityString());
    }

    public static String getVerticesTableName(String tableNamePrefix) {
        return tableNamePrefix + "_v";
    }

    public static String getEdgesTableName(String tableNamePrefix) {
        return tableNamePrefix + "_e";
    }

    public static String getDataTableName(String tableNamePrefix) {
        return tableNamePrefix + "_d";
    }

    private String getVerticesTableName() {
        return getVerticesTableName(getConfiguration().getTableNamePrefix());
    }

    private String getEdgesTableName() {
        return getEdgesTableName(getConfiguration().getTableNamePrefix());
    }

    private String getDataTableName() {
        return getDataTableName(getConfiguration().getTableNamePrefix());
    }
}
