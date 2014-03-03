package com.altamiracorp.securegraph.accumulo;

import com.altamiracorp.securegraph.*;
import com.altamiracorp.securegraph.accumulo.serializer.ValueSerializer;
import com.altamiracorp.securegraph.property.StreamingPropertyValue;
import com.altamiracorp.securegraph.util.LimitOutputStream;
import com.altamiracorp.securegraph.util.StreamUtils;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

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
        m.put(AccumuloVertex.CF_SIGNAL, EMPTY_TEXT, vertex.getGraph().visibilityToAccumuloVisibility(vertex.getVisibility()), EMPTY_VALUE);
        for (Property property : vertex.getProperties()) {
            addPropertyToMutation(vertex.getGraph(), m, vertexRowKey, property);
        }
        return m;
    }

    public void saveEdge(AccumuloEdge edge) {
        ColumnVisibility edgeColumnVisibility = edge.getGraph().visibilityToAccumuloVisibility(edge.getVisibility());
        Mutation m = createMutationForEdge(edge, edgeColumnVisibility);
        saveEdgeMutation(m);

        // Update out vertex.
        Mutation addEdgeToOutMutation = new Mutation(AccumuloConstants.VERTEX_ROW_KEY_PREFIX + edge.getVertexId(Direction.OUT));
        EdgeInfo edgeInfo = new EdgeInfo(edge.getLabel(), edge.getVertexId(Direction.IN));
        addEdgeToOutMutation.put(AccumuloVertex.CF_OUT_EDGE, new Text(edge.getId().toString()), edgeColumnVisibility, valueSerializer.objectToValue(edgeInfo));
        saveVertexMutation(addEdgeToOutMutation);

        // Update in vertex.
        Mutation addEdgeToInMutation = new Mutation(AccumuloConstants.VERTEX_ROW_KEY_PREFIX + edge.getVertexId(Direction.IN));
        edgeInfo = new EdgeInfo(edge.getLabel(), edge.getVertexId(Direction.OUT));
        addEdgeToInMutation.put(AccumuloVertex.CF_IN_EDGE, new Text(edge.getId().toString()), edgeColumnVisibility, valueSerializer.objectToValue(edgeInfo));
        saveVertexMutation(addEdgeToInMutation);
    }

    protected abstract void saveEdgeMutation(Mutation m);

    private Mutation createMutationForEdge(AccumuloEdge edge, ColumnVisibility edgeColumnVisibility) {
        String edgeRowKey = AccumuloConstants.EDGE_ROW_KEY_PREFIX + edge.getId();
        Mutation addEdgeMutation = new Mutation(edgeRowKey);
        addEdgeMutation.put(AccumuloEdge.CF_SIGNAL, new Text(edge.getLabel()), edgeColumnVisibility, ElementMutationBuilder.EMPTY_VALUE);
        addEdgeMutation.put(AccumuloEdge.CF_OUT_VERTEX, new Text(edge.getVertexId(Direction.OUT).toString()), edgeColumnVisibility, ElementMutationBuilder.EMPTY_VALUE);
        addEdgeMutation.put(AccumuloEdge.CF_IN_VERTEX, new Text(edge.getVertexId(Direction.IN).toString()), edgeColumnVisibility, ElementMutationBuilder.EMPTY_VALUE);
        for (Property property : edge.getProperties()) {
            addPropertyToMutation(edge.getGraph(), addEdgeMutation, edgeRowKey, property);
        }
        return addEdgeMutation;
    }

    public void alterElementVisibility(AccumuloGraph graph, Mutation m, AccumuloElement element, Visibility newVisibility) {
        ColumnVisibility currentColumnVisibility = graph.visibilityToAccumuloVisibility(element.getVisibility());
        ColumnVisibility newColumnVisibility = graph.visibilityToAccumuloVisibility(newVisibility);
        if (element instanceof AccumuloEdge) {
            AccumuloEdge edge = (AccumuloEdge) element;
            m.putDelete(AccumuloEdge.CF_SIGNAL, new Text(edge.getLabel()), currentColumnVisibility);
            m.put(AccumuloEdge.CF_SIGNAL, new Text(edge.getLabel()), newColumnVisibility, ElementMutationBuilder.EMPTY_VALUE);
        } else if (element instanceof AccumuloVertex) {
            m.putDelete(AccumuloVertex.CF_SIGNAL, EMPTY_TEXT, currentColumnVisibility);
            m.put(AccumuloVertex.CF_SIGNAL, EMPTY_TEXT, newColumnVisibility, ElementMutationBuilder.EMPTY_VALUE);
        } else {
            throw new IllegalArgumentException("Invalid element type: " + element);
        }
    }

    public void addPropertyToMutation(AccumuloGraph graph, Mutation m, String rowKey, Property property) {
        Text columnQualifier = new Text(property.getName() + VALUE_SEPARATOR + property.getKey());
        ColumnVisibility columnVisibility = graph.visibilityToAccumuloVisibility(property.getVisibility());
        Object propertyValue = property.getValue();
        if (propertyValue instanceof StreamingPropertyValue) {
            StreamingPropertyValueRef streamingPropertyValueRef = saveStreamingPropertyValue(rowKey, property, (StreamingPropertyValue) propertyValue);
            propertyValue = streamingPropertyValueRef;
        }
        if (propertyValue instanceof DateOnly) {
            propertyValue = ((DateOnly) propertyValue).getDate();
        }
        Value value = new Value(valueSerializer.objectToValue(propertyValue));
        m.put(AccumuloElement.CF_PROPERTY, columnQualifier, columnVisibility, value);
        if (property.getMetadata() != null && property.getMetadata().size() > 0) {
            Value metadataValue = new Value(valueSerializer.objectToValue(property.getMetadata()));
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

    public void addPropertyRemoveToMutation(AccumuloGraph graph, Mutation m, Property property) {
        Text columnQualifier = new Text(property.getName() + VALUE_SEPARATOR + property.getKey());
        ColumnVisibility columnVisibility = graph.visibilityToAccumuloVisibility(property.getVisibility());
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
