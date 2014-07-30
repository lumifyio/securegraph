package org.securegraph.accumulo.iterator;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.securegraph.accumulo.EdgeInfo;
import sun.misc.BASE64Decoder;
import sun.misc.BASE64Encoder;

import java.io.*;
import java.util.Map;
import java.util.Set;

public class FindRelatedEdgesFilter extends Filter {
    private static final String OPT_VERTEX_IDS = "vertex.ids";
    private Set<Object> vertexIds;

    @Override
    public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env) throws IOException {
        this.vertexIds = getVertexIdsOption(options);
        super.init(source, options, env);
    }

    private Set<Object> getVertexIdsOption(Map<String, String> options) throws IOException {
        String vertexIdsString = options.get(OPT_VERTEX_IDS);
        try {
            BASE64Decoder decoder = new BASE64Decoder();
            ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(decoder.decodeBuffer(vertexIdsString)));
            return (Set<Object>) in.readObject();
        } catch (ClassNotFoundException ex) {
            throw new IOException("Could not read vertex ids", ex);
        }
    }

    @Override
    public boolean accept(Key k, Value v) {
        EdgeInfo edgeInfo = valueToEdgeInfo(v);
        return this.vertexIds.contains(edgeInfo.getVertexId());
    }

    private EdgeInfo valueToEdgeInfo(Value v) {
        try {
            ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(v.get()));
            try {
                return (EdgeInfo) ois.readObject();
            } finally {
                ois.close();
            }
        } catch (Exception e) {
            throw new RuntimeException("Could not read edge info from value", e);
        }
    }

    public static void setVertexIds(IteratorSetting iteratorSetting, Set<Object> vertexIdsSet) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream out = new ObjectOutputStream(baos);
            out.writeObject(vertexIdsSet);
            out.close();

            BASE64Encoder encoder = new BASE64Encoder();
            iteratorSetting.addOption(OPT_VERTEX_IDS, encoder.encode(baos.toByteArray()));
        } catch (IOException e) {
            throw new RuntimeException("Could not set vertices option", e);
        }
    }
}
