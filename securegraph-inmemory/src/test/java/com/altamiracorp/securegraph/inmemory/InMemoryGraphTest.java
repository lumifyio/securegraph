package com.altamiracorp.securegraph.inmemory;

import com.altamiracorp.securegraph.*;
import com.altamiracorp.securegraph.id.UUIDIdGenerator;
import com.altamiracorp.securegraph.search.DefaultSearchIndex;
import com.altamiracorp.securegraph.test.GraphTestBase;
import org.junit.After;
import org.junit.Before;
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
public class InMemoryGraphTest extends GraphTestBase {
    @Override
    protected Graph createGraph() {
        Map config = new HashMap();
        config.put("", InMemoryGraph.class.getName());
        config.put(GraphConfiguration.IDGENERATOR_PROP_PREFIX, UUIDIdGenerator.class.getName());
        config.put(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX, DefaultSearchIndex.class.getName());
        return new GraphFactory().createGraph(config);
    }

    @Override
    public InMemoryGraph getGraph() {
        return (InMemoryGraph) super.getGraph();
    }

    @Before
    @Override
    public void before() throws Exception {
        super.before();
    }

    @After
    public void after() throws Exception {
        super.after();
    }

    @Test
    public void testSaveAndLoad() throws IOException {
        Map<String, Object> prop1Metadata = new HashMap<String, Object>();
        prop1Metadata.put("metadata1", "metadata1Value");

        Vertex v1 = graph.addVertex("v1", VISIBILITY_A,
                graph.createProperty("id1a", "prop1", "value1a", prop1Metadata, VISIBILITY_A),
                graph.createProperty("id1b", "prop1", "value1b", VISIBILITY_A),
                graph.createProperty("id2", "prop2", "value2", VISIBILITY_B));
        Vertex v2 = graph.addVertex("v2", VISIBILITY_A);
        Vertex v3 = graph.addVertex("v3", VISIBILITY_B);
        graph.addEdge("e1to2", v1, v2, "label1", VISIBILITY_A);
        graph.addEdge("e1to3", v1, v3, "label1", VISIBILITY_B);

        File tmp = File.createTempFile(getClass().getName(), ".json");
        FileOutputStream out = new FileOutputStream(tmp);
        try {
            System.out.println("saving graph to: " + tmp);
            getGraph().save(out);
        } finally {
            out.close();
        }

        FileInputStream in = new FileInputStream(tmp);
        try {
            InMemoryGraph loadedGraph = InMemoryGraph.load(in);

            assertEquals(3, count(loadedGraph.getVertices(AUTHORIZATIONS_A_AND_B)));
            assertEquals(2, count(loadedGraph.getVertices(AUTHORIZATIONS_A)));
            assertEquals(1, count(loadedGraph.getVertices(AUTHORIZATIONS_B)));
            assertEquals(2, count(loadedGraph.getEdges(AUTHORIZATIONS_A_AND_B)));
            assertEquals(1, count(loadedGraph.getEdges(AUTHORIZATIONS_A)));
            assertEquals(1, count(loadedGraph.getEdges(AUTHORIZATIONS_B)));

            v1 = graph.getVertex("v1", AUTHORIZATIONS_A_AND_B);
            assertEquals(2, count(v1.getEdges(Direction.BOTH, AUTHORIZATIONS_A_AND_B)));

            v2 = graph.getVertex("v2", AUTHORIZATIONS_A_AND_B);
            assertEquals(1, count(v2.getEdges(Direction.BOTH, AUTHORIZATIONS_A_AND_B)));

            v3 = graph.getVertex("v3", AUTHORIZATIONS_A_AND_B);
            assertEquals(1, count(v3.getEdges(Direction.BOTH, AUTHORIZATIONS_A_AND_B)));
        } finally {
            in.close();
        }

        tmp.delete();
    }
}
