package com.altamiracorp.securegraph.accumulo;

import com.altamiracorp.securegraph.*;
import com.altamiracorp.securegraph.accumulo.search.SearchIndex;
import com.altamiracorp.securegraph.accumulo.serializer.ValueSerializer;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;

public class AccumuloGraph extends GraphBase {
    private static final Text EMPTY_TEXT = new Text("");
    private static final Value EMPTY_VALUE = new Value(new byte[0]);
    private final Connector connector;
    private final ValueSerializer valueSerializer;
    private final SearchIndex searchIndex;
    private BatchWriter writer;
    private final Object writerLock = new Object();

    @Override
    public Vertex addVertex(Object vertexId, Visibility vertexVisibility, Property... properties) {
        AccumuloVertex vertex = new AccumuloVertex(this, vertexId, vertexVisibility);

        Mutation m = new Mutation(vertex.getRowKey());
        m.put(AccumuloVertex.CF_SIGNAL, EMPTY_TEXT, new ColumnVisibility(vertexVisibility.getVisibilityString()), EMPTY_VALUE);
        for (Property property : properties) {
            addPropertyToMutation(m, property);
        }
        addMutations(m);

        getSearchIndex().addElement(vertex);

        return vertex;
    }

    private void addPropertyToMutation(Mutation m, Property property) {
        Text cf = AccumuloElement.CF_PROPERTY;
        Text columnQualifier = new Text(property.getName());
        ColumnVisibility columnVisibility = new ColumnVisibility(property.getVisibility().getVisibilityString());
        Value value = new Value(getValueSerializer().objectToValue(property.getValue()));
        m.put(cf, columnQualifier, columnVisibility, value);
    }

    private void addMutations(Mutation... mutations) {
        try {
            BatchWriter writer = getWriter();
            synchronized (this.writerLock) {
                for (Mutation m : mutations) {
                    writer.addMutation(m);
                }
                if (config.isAutoFlush()) {
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
            this.writer = this.connector.createBatchWriter(config.getTableName(), writerConfig);
            return this.writer;
        } catch (TableNotFoundException ex) {
            throw new RuntimeException("Could not create batch writer", ex);
        }
    }

    @Override
    public Iterable<Vertex> getVertices(Authorizations authorizations) {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void removeVertex(Object vertexId, Authorizations authorizations) {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public Edge addEdge(Object edgeId, Vertex outVertex, Vertex inVertex, String label, Visibility visibility, Property... properties) {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public Iterable<Edge> getEdges(Authorizations authorizations) {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void removeEdge(Object edgeId, Authorizations authorizations) {
        throw new RuntimeException("Not implemented");
    }

    public ValueSerializer getValueSerializer() {
        return valueSerializer;
    }

    public SearchIndex getSearchIndex() {
        return searchIndex;
    }
}
