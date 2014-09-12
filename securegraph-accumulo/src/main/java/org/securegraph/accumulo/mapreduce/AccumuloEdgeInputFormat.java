package org.securegraph.accumulo.mapreduce;

import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.PeekingIterator;
import org.apache.hadoop.mapreduce.Job;
import org.securegraph.Authorizations;
import org.securegraph.Edge;
import org.securegraph.accumulo.AccumuloGraph;
import org.securegraph.accumulo.EdgeMaker;

import java.util.Map;
import java.util.SortedMap;

public class AccumuloEdgeInputFormat extends AccumuloElementInputFormatBase<Edge> {
    public static void setInputInfo(Job job, AccumuloGraph graph, String instanceName, String zooKeepers, String principal, AuthenticationToken token, String[] authorizations) throws AccumuloSecurityException {
        String tableName = graph.getEdgesTableName();
        setInputInfo(job, instanceName, zooKeepers, principal, token, authorizations, tableName);
    }

    @Override
    protected Edge createElementFromRow(AccumuloGraph graph, PeekingIterator<Map.Entry<Key, Value>> row, Authorizations authorizations) {
        EdgeMaker maker = new EdgeMaker(graph, row, authorizations);
        return maker.make();
    }
}

