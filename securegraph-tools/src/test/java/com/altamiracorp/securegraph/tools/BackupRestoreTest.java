package com.altamiracorp.securegraph.tools;

import com.altamiracorp.securegraph.*;
import com.altamiracorp.securegraph.id.UUIDIdGenerator;
import com.altamiracorp.securegraph.inmemory.InMemoryGraph;
import com.altamiracorp.securegraph.search.DefaultSearchIndex;
import com.altamiracorp.securegraph.test.GraphTestBase;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static com.altamiracorp.securegraph.test.util.IterableUtils.count;
import static org.junit.Assert.assertEquals;

@RunWith(JUnit4.class)
public class BackupRestoreTest {
    protected Graph createGraph() {
        Map config = new HashMap();
        config.put("", InMemoryGraph.class.getName());
        config.put(GraphConfiguration.IDGENERATOR_PROP_PREFIX, UUIDIdGenerator.class.getName());
        config.put(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX, DefaultSearchIndex.class.getName());
        return new GraphFactory().createGraph(config);
    }

    @Test
    public void testSaveAndLoad() throws IOException {
        Graph graph = createGraph();

        Map<String, Object> prop1Metadata = new HashMap<String, Object>();
        prop1Metadata.put("metadata1", "metadata1Value");

        Vertex v1 = graph.prepareVertex("v1", GraphTestBase.VISIBILITY_A)
                .addPropertyValue("id1a", "prop1", "value1a", prop1Metadata, GraphTestBase.VISIBILITY_A)
                .addPropertyValue("id1b", "prop1", "value1b", GraphTestBase.VISIBILITY_A)
                .addPropertyValue("id2", "prop2", "value2", GraphTestBase.VISIBILITY_B)
                .save();
        Vertex v2 = graph.addVertex("v2", GraphTestBase.VISIBILITY_A);
        Vertex v3 = graph.addVertex("v3", GraphTestBase.VISIBILITY_B);
        graph.addEdge("e1to2", v1, v2, "label1", GraphTestBase.VISIBILITY_A);
        graph.addEdge("e1to3", v1, v3, "label1", GraphTestBase.VISIBILITY_B);

        File tmp = File.createTempFile(getClass().getName(), ".json");
        FileOutputStream out = new FileOutputStream(tmp);
        try {
            System.out.println("saving graph to: " + tmp);
            GraphBackup graphBackup = new GraphBackup();
            graphBackup.save(graph, out, GraphTestBase.AUTHORIZATIONS_A_AND_B);
        } finally {
            out.close();
        }

        FileInputStream in = new FileInputStream(tmp);
        try {
            Graph loadedGraph = createGraph();
            GraphRestore graphRestore = new GraphRestore();
            graphRestore.restore(loadedGraph, in, GraphTestBase.AUTHORIZATIONS_A_AND_B);

            assertEquals(3, count(loadedGraph.getVertices(GraphTestBase.AUTHORIZATIONS_A_AND_B)));
            assertEquals(2, count(loadedGraph.getVertices(GraphTestBase.AUTHORIZATIONS_A)));
            assertEquals(1, count(loadedGraph.getVertices(GraphTestBase.AUTHORIZATIONS_B)));
            assertEquals(2, count(loadedGraph.getEdges(GraphTestBase.AUTHORIZATIONS_A_AND_B)));
            assertEquals(1, count(loadedGraph.getEdges(GraphTestBase.AUTHORIZATIONS_A)));
            assertEquals(1, count(loadedGraph.getEdges(GraphTestBase.AUTHORIZATIONS_B)));

            v1 = graph.getVertex("v1", GraphTestBase.AUTHORIZATIONS_A_AND_B);
            assertEquals(2, count(v1.getEdges(Direction.BOTH, GraphTestBase.AUTHORIZATIONS_A_AND_B)));

            v2 = graph.getVertex("v2", GraphTestBase.AUTHORIZATIONS_A_AND_B);
            assertEquals(1, count(v2.getEdges(Direction.BOTH, GraphTestBase.AUTHORIZATIONS_A_AND_B)));

            v3 = graph.getVertex("v3", GraphTestBase.AUTHORIZATIONS_A_AND_B);
            assertEquals(1, count(v3.getEdges(Direction.BOTH, GraphTestBase.AUTHORIZATIONS_A_AND_B)));
        } finally {
            in.close();
        }

        tmp.delete();
    }
}
