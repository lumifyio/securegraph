package org.securegraph.accumulo;

import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.securegraph.*;
import org.securegraph.accumulo.serializer.ValueSerializer;
import org.securegraph.property.StreamingPropertyValue;
import org.securegraph.util.LimitOutputStream;
import org.securegraph.util.StreamUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

import static org.securegraph.util.Preconditions.checkNotNull;

public abstract class ElementMutationBuilder {
    private static final Logger LOGGER = LoggerFactory.getLogger(ElementMutationBuilder.class);
    private static final Text EMPTY_TEXT = new Text("");
    public static final Value EMPTY_VALUE = new Value(new byte[0]);
    public static final String VALUE_SEPARATOR = "\u001f";

    private final FileSystem fileSystem;
    private final ValueSerializer valueSerializer;
    private final long maxStreamingPropertyValueTableDataSize;
    private final String dataDir;

    protected ElementMutationBuilder(FileSystem fileSystem, ValueSerializer valueSerializer, long maxStreamingPropertyValueTableDataSize, String dataDir) {
        this.fileSystem = fileSystem;
        this.valueSerializer = valueSerializer;
        this.maxStreamingPropertyValueTableDataSize = maxStreamingPropertyValueTableDataSize;
        this.dataDir = dataDir;
    }

    public void saveVertex(AccumuloVertex vertex) {
        Mutation m = createMutationForVertex(vertex);
        saveVertexMutation(m);
    }

    protected abstract void saveVertexMutation(Mutation m);

    private Mutation createMutationForVertex(AccumuloVertex vertex) {
        String vertexRowKey = AccumuloConstants.VERTEX_ROW_KEY_PREFIX + vertex.getId();
        Mutation m = new Mutation(vertexRowKey);
        m.put(AccumuloVertex.CF_SIGNAL, EMPTY_TEXT, visibilityToAccumuloVisibility(vertex.getVisibility()), EMPTY_VALUE);
        for (Property property : vertex.getProperties()) {
            addPropertyToMutation(m, vertexRowKey, property);
        }
        return m;
    }

    public void saveEdge(AccumuloEdge edge) {
        ColumnVisibility edgeColumnVisibility = visibilityToAccumuloVisibility(edge.getVisibility());
        Mutation m = createMutationForEdge(edge, edgeColumnVisibility);
        saveEdgeMutation(m);

        // Update out vertex.
        Mutation addEdgeToOutMutation = new Mutation(AccumuloConstants.VERTEX_ROW_KEY_PREFIX + edge.getVertexId(Direction.OUT));
        EdgeInfo edgeInfo = new EdgeInfo(edge.getLabel(), edge.getVertexId(Direction.IN));
        addEdgeToOutMutation.put(AccumuloVertex.CF_OUT_EDGE, new Text(edge.getId()), edgeColumnVisibility, edgeInfo.toValue());
        saveVertexMutation(addEdgeToOutMutation);

        // Update in vertex.
        Mutation addEdgeToInMutation = new Mutation(AccumuloConstants.VERTEX_ROW_KEY_PREFIX + edge.getVertexId(Direction.IN));
        edgeInfo = new EdgeInfo(edge.getLabel(), edge.getVertexId(Direction.OUT));
        addEdgeToInMutation.put(AccumuloVertex.CF_IN_EDGE, new Text(edge.getId()), edgeColumnVisibility, edgeInfo.toValue());
        saveVertexMutation(addEdgeToInMutation);
    }

    private ColumnVisibility visibilityToAccumuloVisibility(Visibility visibility) {
        return new ColumnVisibility(visibility.getVisibilityString());
    }

    protected abstract void saveEdgeMutation(Mutation m);

    private Mutation createMutationForEdge(AccumuloEdge edge, ColumnVisibility edgeColumnVisibility) {
        String edgeRowKey = AccumuloConstants.EDGE_ROW_KEY_PREFIX + edge.getId();
        Mutation addEdgeMutation = new Mutation(edgeRowKey);
        addEdgeMutation.put(AccumuloEdge.CF_SIGNAL, new Text(edge.getLabel()), edgeColumnVisibility, ElementMutationBuilder.EMPTY_VALUE);
        addEdgeMutation.put(AccumuloEdge.CF_OUT_VERTEX, new Text(edge.getVertexId(Direction.OUT)), edgeColumnVisibility, ElementMutationBuilder.EMPTY_VALUE);
        addEdgeMutation.put(AccumuloEdge.CF_IN_VERTEX, new Text(edge.getVertexId(Direction.IN)), edgeColumnVisibility, ElementMutationBuilder.EMPTY_VALUE);
        for (Property property : edge.getProperties()) {
            addPropertyToMutation(addEdgeMutation, edgeRowKey, property);
        }
        return addEdgeMutation;
    }

    public boolean alterElementVisibility(Mutation m, AccumuloElement element, Visibility newVisibility) {
        ColumnVisibility currentColumnVisibility = visibilityToAccumuloVisibility(element.getVisibility());
        ColumnVisibility newColumnVisibility = visibilityToAccumuloVisibility(newVisibility);
        if (currentColumnVisibility.equals(newColumnVisibility)) {
            return false;
        }

        if (element instanceof AccumuloEdge) {
            AccumuloEdge edge = (AccumuloEdge) element;
            m.putDelete(AccumuloEdge.CF_SIGNAL, new Text(edge.getLabel()), currentColumnVisibility);
            m.put(AccumuloEdge.CF_SIGNAL, new Text(edge.getLabel()), newColumnVisibility, ElementMutationBuilder.EMPTY_VALUE);

            m.putDelete(AccumuloEdge.CF_OUT_VERTEX, new Text(edge.getVertexId(Direction.OUT)), currentColumnVisibility);
            m.put(AccumuloEdge.CF_OUT_VERTEX, new Text(edge.getVertexId(Direction.OUT)), newColumnVisibility, ElementMutationBuilder.EMPTY_VALUE);

            m.putDelete(AccumuloEdge.CF_IN_VERTEX, new Text(edge.getVertexId(Direction.IN)), currentColumnVisibility);
            m.put(AccumuloEdge.CF_IN_VERTEX, new Text(edge.getVertexId(Direction.IN)), newColumnVisibility, ElementMutationBuilder.EMPTY_VALUE);
        } else if (element instanceof AccumuloVertex) {
            m.putDelete(AccumuloVertex.CF_SIGNAL, EMPTY_TEXT, currentColumnVisibility);
            m.put(AccumuloVertex.CF_SIGNAL, EMPTY_TEXT, newColumnVisibility, ElementMutationBuilder.EMPTY_VALUE);
        } else {
            throw new IllegalArgumentException("Invalid element type: " + element);
        }
        return true;
    }

    public boolean alterEdgeVertexOutVertex(Mutation mvout, Edge edge, Visibility newVisibility) {
        ColumnVisibility currentColumnVisibility = visibilityToAccumuloVisibility(edge.getVisibility());
        ColumnVisibility newColumnVisibility = visibilityToAccumuloVisibility(newVisibility);
        if (currentColumnVisibility.equals(newColumnVisibility)) {
            return false;
        }
        EdgeInfo edgeInfo = new EdgeInfo(edge.getLabel(), edge.getVertexId(Direction.IN));
        mvout.putDelete(AccumuloVertex.CF_OUT_EDGE, new Text(edge.getId()), currentColumnVisibility);
        mvout.put(AccumuloVertex.CF_OUT_EDGE, new Text(edge.getId()), newColumnVisibility, edgeInfo.toValue());
        return true;
    }

    public boolean alterEdgeVertexInVertex(Mutation mvin, Edge edge, Visibility newVisibility) {
        ColumnVisibility currentColumnVisibility = visibilityToAccumuloVisibility(edge.getVisibility());
        ColumnVisibility newColumnVisibility = visibilityToAccumuloVisibility(newVisibility);
        if (currentColumnVisibility.equals(newColumnVisibility)) {
            return false;
        }
        EdgeInfo edgeInfo = new EdgeInfo(edge.getLabel(), edge.getVertexId(Direction.OUT));
        mvin.putDelete(AccumuloVertex.CF_IN_EDGE, new Text(edge.getId()), currentColumnVisibility);
        mvin.put(AccumuloVertex.CF_IN_EDGE, new Text(edge.getId()), newColumnVisibility, edgeInfo.toValue());
        return true;
    }

    public void addPropertyToMutation(Mutation m, String rowKey, Property property) {
        Text columnQualifier = getPropertyColumnQualifier(property);
        ColumnVisibility columnVisibility = visibilityToAccumuloVisibility(property.getVisibility());
        Object propertyValue = property.getValue();
        if (propertyValue instanceof StreamingPropertyValue) {
            propertyValue = saveStreamingPropertyValue(rowKey, property, (StreamingPropertyValue) propertyValue);
        }
        if (propertyValue instanceof DateOnly) {
            propertyValue = ((DateOnly) propertyValue).getDate();
        }
        Value value = new Value(valueSerializer.objectToValue(propertyValue));
        m.put(AccumuloElement.CF_PROPERTY, columnQualifier, columnVisibility, value);
        addPropertyMetadataToMutation(m, property);
    }

    static Text getPropertyColumnQualifier(Property property) {
        return new Text(property.getName() + VALUE_SEPARATOR + property.getKey());
    }

    static Text getPropertyColumnQualifierWithVisibilityString(Property property) {
        return new Text(property.getName() + VALUE_SEPARATOR + property.getKey() + VALUE_SEPARATOR + property.getVisibility().getVisibilityString());
    }

    public void addPropertyMetadataToMutation(Mutation m, Property property) {
        Map<String, Object> metadata = property.getMetadata();
        Text columnQualifier = getPropertyColumnQualifier(property);
        ColumnVisibility columnVisibility = visibilityToAccumuloVisibility(property.getVisibility());
        if (metadata != null && metadata.size() > 0) {
            Value metadataValue = new Value(valueSerializer.objectToValue(metadata));
            m.put(AccumuloElement.CF_PROPERTY_METADATA, columnQualifier, columnVisibility, metadataValue);
        } else {
            m.put(AccumuloElement.CF_PROPERTY_METADATA, columnQualifier, columnVisibility, EMPTY_VALUE);
        }
    }

    protected StreamingPropertyValueRef saveStreamingPropertyValue(final String rowKey, final Property property, StreamingPropertyValue propertyValue) {
        try {
            HdfsLargeDataStore largeDataStore = new HdfsLargeDataStore(this.fileSystem, this.dataDir, rowKey, property);
            LimitOutputStream out = new LimitOutputStream(largeDataStore, maxStreamingPropertyValueTableDataSize);
            try {
                StreamUtils.copy(propertyValue.getInputStream(), out);
            } finally {
                out.close();
            }

            if (out.hasExceededSizeLimit()) {
                LOGGER.debug(String.format("saved large file to \"%s\" (length: %d)", largeDataStore.getFullHdfsPath(), out.getLength()));
                return new StreamingPropertyValueHdfsRef(largeDataStore.getRelativeFileName(), propertyValue);
            } else {
                return saveStreamingPropertyValueSmall(rowKey, property, out.getSmall(), propertyValue);
            }
        } catch (IOException ex) {
            throw new SecureGraphException(ex);
        }
    }

    public void addPropertyRemoveToMutation(Mutation m, Property property) {
        checkNotNull(m, "mutation cannot be null");
        checkNotNull(property, "property cannot be null");
        Text columnQualifier = getPropertyColumnQualifier(property);
        ColumnVisibility columnVisibility = visibilityToAccumuloVisibility(property.getVisibility());
        m.putDelete(AccumuloElement.CF_PROPERTY, columnQualifier, columnVisibility);
        m.putDelete(AccumuloElement.CF_PROPERTY_METADATA, columnQualifier, columnVisibility);
    }

    private StreamingPropertyValueRef saveStreamingPropertyValueSmall(String rowKey, Property property, byte[] data, StreamingPropertyValue propertyValue) {
        String dataRowKey = createTableDataRowKey(rowKey, property);
        Mutation dataMutation = new Mutation(dataRowKey);
        dataMutation.put(EMPTY_TEXT, EMPTY_TEXT, new Value(data));
        saveDataMutation(dataMutation);
        return new StreamingPropertyValueTableRef(dataRowKey, propertyValue, data);
    }

    protected abstract void saveDataMutation(Mutation dataMutation);

    private String createTableDataRowKey(String rowKey, Property property) {
        return AccumuloConstants.DATA_ROW_KEY_PREFIX + rowKey + VALUE_SEPARATOR + property.getName() + VALUE_SEPARATOR + property.getKey();
    }
}
