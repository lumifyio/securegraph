package com.altamiracorp.securegraph.tools;

import com.altamiracorp.securegraph.*;
import com.altamiracorp.securegraph.id.UUIDIdGenerator;
import com.altamiracorp.securegraph.inmemory.InMemoryGraph;
import com.altamiracorp.securegraph.property.StreamingPropertyValue;
import com.altamiracorp.securegraph.search.DefaultSearchIndex;
import com.altamiracorp.securegraph.test.GraphTestBase;
import com.altamiracorp.securegraph.test.util.LargeStringInputStream;
import org.apache.commons.io.IOUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

import static com.altamiracorp.securegraph.test.util.IterableUtils.count;
import static org.junit.Assert.*;

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
    public void testSaveAndLoad() throws IOException, ClassNotFoundException {
        Graph graph = createGraph();

        Map<String, Object> prop1Metadata = new HashMap<String, Object>();
        prop1Metadata.put("metadata1", "metadata1Value");

        int largePropertyValueSize = 1000;
        String expectedLargeValue = IOUtils.toString(new LargeStringInputStream(largePropertyValueSize));
        StreamingPropertyValue largeDataValue = new StreamingPropertyValue(new ByteArrayInputStream(expectedLargeValue.getBytes()), String.class);

        Vertex v1 = graph.prepareVertex("v1", GraphTestBase.VISIBILITY_A)
                .addPropertyValue("id1a", "prop1", "value1a", prop1Metadata, GraphTestBase.VISIBILITY_A)
                .addPropertyValue("id1b", "prop1", "value1b", GraphTestBase.VISIBILITY_A)
                .addPropertyValue("id2", "prop2", "value2", GraphTestBase.VISIBILITY_B)
                .setProperty("largeData", largeDataValue, GraphTestBase.VISIBILITY_A)
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

            v1 = loadedGraph.getVertex("v1", GraphTestBase.AUTHORIZATIONS_A_AND_B);
            assertEquals(2, count(v1.getEdges(Direction.BOTH, GraphTestBase.AUTHORIZATIONS_A_AND_B)));
            Iterable<Property> properties = v1.getProperties();
            boolean prop1_id1a_found = false;
            boolean prop1_id1b_found = false;
            for (Property property : properties) {
                if (property.getName().equals("prop1")) {
                    if (property.getId().equals("id1a")) {
                        prop1_id1a_found = true;
                        assertEquals("value1a", property.getValue());
                    }
                    if (property.getId().equals("id1b")) {
                        prop1_id1b_found = true;
                        assertEquals("value1b", property.getValue());
                    }
                }
            }
            assertTrue("prop1[id1a] not found", prop1_id1a_found);
            assertTrue("prop1[id1b] not found", prop1_id1b_found);
            assertEquals("value2", v1.getPropertyValue("prop2", 0));
            StreamingPropertyValue spv = (StreamingPropertyValue) v1.getPropertyValue("largeData", 0);
            assertNotNull("largeData property not found", spv);
            assertEquals(String.class, spv.getValueType());
            assertEquals(expectedLargeValue, IOUtils.toString(spv.getInputStream()));

            v2 = loadedGraph.getVertex("v2", GraphTestBase.AUTHORIZATIONS_A_AND_B);
            assertEquals(1, count(v2.getEdges(Direction.BOTH, GraphTestBase.AUTHORIZATIONS_A_AND_B)));

            v3 = loadedGraph.getVertex("v3", GraphTestBase.AUTHORIZATIONS_A_AND_B);
            assertEquals(1, count(v3.getEdges(Direction.BOTH, GraphTestBase.AUTHORIZATIONS_A_AND_B)));
        } finally {
            in.close();
        }

        tmp.delete();
    }
}
