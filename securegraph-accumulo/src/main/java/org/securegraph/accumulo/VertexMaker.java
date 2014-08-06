package org.securegraph.accumulo;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.securegraph.Authorizations;
import org.securegraph.SecureGraphException;
import org.securegraph.Vertex;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class VertexMaker extends ElementMaker<Vertex> {
    private static final String VISIBILITY_SIGNAL = AccumuloVertex.CF_SIGNAL.toString();

    private final AccumuloGraph graph;
    private final Map<String, EdgeInfo> outEdges = new HashMap<String, EdgeInfo>();
    private final Map<String, EdgeInfo> inEdges = new HashMap<String, EdgeInfo>();

    public VertexMaker(AccumuloGraph graph, Iterator<Map.Entry<Key, Value>> row, Authorizations authorizations) {
        super(graph, row, authorizations);
        this.graph = graph;
    }

    @Override
    protected void processColumn(Key key, Value value) {
        Text columnFamily = key.getColumnFamily();
        Text columnQualifier = key.getColumnQualifier();

        if (AccumuloVertex.CF_OUT_EDGE.compareTo(columnFamily) == 0) {
            String edgeId = columnQualifier.toString();
            EdgeInfo edgeInfo = EdgeInfo.parse(value);
            outEdges.put(edgeId, edgeInfo);
            return;
        }

        if (AccumuloVertex.CF_IN_EDGE.compareTo(columnFamily) == 0) {
            String edgeId = columnQualifier.toString();
            EdgeInfo edgeInfo = EdgeInfo.parse(value);
            inEdges.put(edgeId, edgeInfo);
            return;
        }
    }

    @Override
    protected String getIdFromRowKey(String rowKey) throws SecureGraphException {
        if (rowKey.startsWith(AccumuloConstants.VERTEX_ROW_KEY_PREFIX)) {
            return rowKey.substring(AccumuloConstants.VERTEX_ROW_KEY_PREFIX.length());
        }
        throw new SecureGraphException("Invalid row key for vertex: " + rowKey);
    }

    @Override
    protected String getVisibilitySignal() {
        return VISIBILITY_SIGNAL;
    }

    @Override
    protected Vertex makeElement() {
        return new AccumuloVertex(
                this.graph,
                this.getId(),
                this.getVisibility(),
                this.getProperties(),
                this.inEdges,
                this.outEdges,
                this.getAuthorizations());
    }

}
