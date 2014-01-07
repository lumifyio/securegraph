package com.altamiracorp.securegraph.accumulo;

import com.altamiracorp.securegraph.Edge;
import com.altamiracorp.securegraph.SecureGraphException;
import com.altamiracorp.securegraph.accumulo.serializer.ValueSerializer;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;

import java.util.Iterator;
import java.util.Map;

public class EdgeMaker extends ElementMaker<Edge> {
    private static final String VISIBILITY_SIGNAL = AccumuloEdge.CF_SIGNAL.toString();
    private final AccumuloGraph graph;
    private String inVertexId;
    private String outVertexId;
    private String label;

    public EdgeMaker(AccumuloGraph graph, ValueSerializer valueSerializer, Iterator<Map.Entry<Key, Value>> row) {
        super(valueSerializer, row);
        this.graph = graph;
    }

    @Override
    protected void processColumn(Key key, Value value) {
        Text columnFamily = key.getColumnFamily();
        Text columnQualifier = key.getColumnQualifier();

        if (AccumuloEdge.CF_SIGNAL.compareTo(columnFamily) == 0) {
            this.label = columnQualifier.toString();
            return;
        }

        if (AccumuloEdge.CF_IN_VERTEX.compareTo(columnFamily) == 0) {
            this.inVertexId = columnQualifier.toString();
            return;
        }

        if (AccumuloEdge.CF_OUT_VERTEX.compareTo(columnFamily) == 0) {
            this.outVertexId = columnQualifier.toString();
        }
    }

    @Override
    protected String getIdFromRowKey(String rowKey) {
        if (rowKey.startsWith(AccumuloEdge.ROW_KEY_PREFIX)) {
            return rowKey.substring(AccumuloEdge.ROW_KEY_PREFIX.length());
        }
        throw new SecureGraphException("Invalid row key for edge: " + rowKey);
    }

    @Override
    protected String getVisibilitySignal() {
        return VISIBILITY_SIGNAL;
    }

    @Override
    protected Edge makeElement() {
        return new AccumuloEdge(this.graph, this.getId(), this.outVertexId, this.inVertexId, this.label, this.getVisibility(), this.getProperties());
    }
}
