package com.altamiracorp.securegraph.accumulo;

import com.altamiracorp.securegraph.SecureGraphException;
import com.altamiracorp.securegraph.Vertex;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

public class VertexMaker extends ElementMaker<Vertex> {
    private static final String VISIBILITY_SIGNAL = AccumuloVertex.CF_SIGNAL.toString();

    private final AccumuloGraph graph;
    private final HashSet<Object> outEdgeIds = new HashSet<Object>();
    private final HashSet<Object> inEdgeIds = new HashSet<Object>();

    public VertexMaker(AccumuloGraph graph, Iterator<Map.Entry<Key, Value>> row) {
        super(graph, row);
        this.graph = graph;
    }

    @Override
    protected void processColumn(Key key, Value value) {
        Text columnFamily = key.getColumnFamily();
        Text columnQualifier = key.getColumnQualifier();

        if (AccumuloVertex.CF_OUT_EDGE.compareTo(columnFamily) == 0) {
            outEdgeIds.add(columnQualifier.toString());
            return;
        }

        if (AccumuloVertex.CF_IN_EDGE.compareTo(columnFamily) == 0) {
            inEdgeIds.add(columnQualifier.toString());
        }
    }

    @Override
    protected String getIdFromRowKey(String rowKey) throws SecureGraphException {
        if (rowKey.startsWith(AccumuloVertex.ROW_KEY_PREFIX)) {
            return rowKey.substring(AccumuloVertex.ROW_KEY_PREFIX.length());
        }
        throw new SecureGraphException("Invalid row key for vertex: " + rowKey);
    }

    @Override
    protected String getVisibilitySignal() {
        return VISIBILITY_SIGNAL;
    }

    @Override
    protected Vertex makeElement() {
        return new AccumuloVertex(this.graph, this.getId(), this.getVisibility(), this.getProperties(), this.inEdgeIds, this.outEdgeIds);
    }

}
