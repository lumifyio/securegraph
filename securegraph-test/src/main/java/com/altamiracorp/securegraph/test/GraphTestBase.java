package com.altamiracorp.securegraph.test;

import com.altamiracorp.securegraph.*;
import com.altamiracorp.securegraph.query.Compare;
import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import static com.altamiracorp.securegraph.test.util.IterableUtils.assertContains;
import static com.altamiracorp.securegraph.test.util.IterableUtils.count;
import static org.junit.Assert.*;

@RunWith(JUnit4.class)
public abstract class GraphTestBase {
    private static final Visibility VISIBILITY_A = new Visibility("a");
    private static final Visibility VISIBILITY_B = new Visibility("b");
    private static final Authorizations AUTHORIZATIONS_A = new Authorizations("a");
    private static final Authorizations AUTHORIZATIONS_B = new Authorizations("b");
    private static final Authorizations AUTHORIZATIONS_C = new Authorizations("c");
    private static final Authorizations AUTHORIZATIONS_A_AND_B = new Authorizations("a", "b");

    private Graph graph;

    protected abstract Graph createGraph() throws Exception;

    @Before
    public void before() throws Exception {
        graph = createGraph();
    }

    @After
    public void after() throws Exception {
        graph = null;
    }

    @Test
    public void testAddVertexWithId() {
        Vertex v = graph.addVertex("v1", VISIBILITY_A);
        assertNotNull(v);
        assertEquals("v1", v.getId());

        v = graph.getVertex("v1", AUTHORIZATIONS_A);
        assertNotNull(v);
        assertEquals("v1", v.getId());
        assertEquals(VISIBILITY_A, v.getVisibility());
    }

    @Test
    public void testAddVertexWithoutId() {
        Vertex v = graph.addVertex(VISIBILITY_A);
        assertNotNull(v);
        Object vertexId = v.getId();
        assertNotNull(vertexId);

        v = graph.getVertex(vertexId, AUTHORIZATIONS_A);
        assertNotNull(v);
        assertNotNull(vertexId);
    }

    @Test
    public void testAddBigDataProperty() throws IOException {
        graph.addVertex("v1", VISIBILITY_A,
                new Property("prop1", new StreamingPropertyValue(new ByteArrayInputStream("value1".getBytes()), "text/plain"), VISIBILITY_A));

        Vertex v1 = graph.getVertex("v1", AUTHORIZATIONS_A);
        Iterable<Object> prop1Values = v1.getPropertyValues("prop1");
        assertEquals(1, count(prop1Values));
        Object val = prop1Values.iterator().next();
        assertTrue(val instanceof StreamingPropertyValue);
        StreamingPropertyValue value = (StreamingPropertyValue) val;
        assertEquals("text/plain", value.getContentType());
        assertEquals("value1", IOUtils.toString(value.getInputStream()));
    }

    @Test
    public void testAddVertexPropertyWithMetadata() {
        Map<String, Object> prop1Metadata = new HashMap<String, Object>();
        prop1Metadata.put("metadata1", "metadata1Value");

        graph.addVertex("v1", VISIBILITY_A,
                new Property("", "prop1", "value1", VISIBILITY_A, prop1Metadata));

        Vertex v = graph.getVertex("v1", AUTHORIZATIONS_A);
        assertEquals(1, count(v.getProperties("prop1")));
        Property prop1 = v.getProperties("prop1").iterator().next();
        prop1Metadata = prop1.getMetadata();
        assertNotNull(prop1Metadata);
        assertEquals(1, prop1Metadata.keySet().size());
        assertEquals("metadata1Value", prop1Metadata.get("metadata1"));

        prop1Metadata.put("metadata2", "metadata2Value");
        v.setProperties(new Property("", "prop1", "value1", VISIBILITY_A, prop1Metadata));

        v = graph.getVertex("v1", AUTHORIZATIONS_A);
        assertEquals(1, count(v.getProperties("prop1")));
        prop1 = v.getProperties("prop1").iterator().next();
        prop1Metadata = prop1.getMetadata();
        assertEquals(2, prop1Metadata.keySet().size());
        assertEquals("metadata1Value", prop1Metadata.get("metadata1"));
        assertEquals("metadata2Value", prop1Metadata.get("metadata2"));
    }

    @Test
    public void testAddVertexWithProperties() {
        Vertex v = graph.addVertex("v1", VISIBILITY_A,
                new Property("prop1", "value1", VISIBILITY_A),
                new Property("prop2", "value2", VISIBILITY_B));
        assertEquals(1, count(v.getProperties("prop1")));
        assertEquals("value1", v.getPropertyValues("prop1").iterator().next());
        assertEquals(1, count(v.getProperties("prop2")));
        assertEquals("value2", v.getPropertyValues("prop2").iterator().next());

        v = graph.getVertex("v1", AUTHORIZATIONS_A_AND_B);
        assertEquals(1, count(v.getProperties("prop1")));
        assertEquals("value1", v.getPropertyValues("prop1").iterator().next());
        assertEquals(1, count(v.getProperties("prop2")));
        assertEquals("value2", v.getPropertyValues("prop2").iterator().next());
    }

    @Test
    public void testMultivaluedProperties() {
        Vertex v = graph.addVertex("v1", VISIBILITY_A);

        v.setProperties(
                new Property("propid1a", "prop1", "value1a", VISIBILITY_A),
                new Property("propid2a", "prop2", "value2a", VISIBILITY_A),
                new Property("propid3a", "prop3", "value3a", VISIBILITY_A));
        v = graph.getVertex("v1", AUTHORIZATIONS_A);
        assertEquals("value1a", v.getPropertyValues("prop1").iterator().next());
        assertEquals("value2a", v.getPropertyValues("prop2").iterator().next());
        assertEquals("value3a", v.getPropertyValues("prop3").iterator().next());
        assertEquals(3, count(v.getProperties()));

        v.setProperties(
                new Property("propid1a", "prop1", "value1b", VISIBILITY_A),
                new Property("propid2a", "prop2", "value2b", VISIBILITY_A));
        v = graph.getVertex("v1", AUTHORIZATIONS_A);
        assertEquals(1, count(v.getPropertyValues("prop1")));
        assertEquals("value1b", v.getPropertyValues("prop1").iterator().next());
        assertEquals(1, count(v.getPropertyValues("prop2")));
        assertEquals("value2b", v.getPropertyValues("prop2").iterator().next());
        assertEquals(1, count(v.getPropertyValues("prop3")));
        assertEquals("value3a", v.getPropertyValues("prop3").iterator().next());
        assertEquals(3, count(v.getProperties()));

        v.setProperties(new Property("propid1b", "prop1", "value1a-new", VISIBILITY_A));
        v = graph.getVertex("v1", AUTHORIZATIONS_A);
        assertContains("value1b", v.getPropertyValues("prop1"));
        assertContains("value1a-new", v.getPropertyValues("prop1"));
        assertEquals(4, count(v.getProperties()));
    }

    @Test
    public void testAddVertexWithVisibility() {
        graph.addVertex("v1", VISIBILITY_A);
        graph.addVertex("v2", VISIBILITY_B);

        Iterable<Vertex> cVertices = graph.getVertices(AUTHORIZATIONS_C);
        assertEquals(0, count(cVertices));

        Iterable<Vertex> aVertices = graph.getVertices(AUTHORIZATIONS_A);
        assertEquals(1, count(aVertices));
        assertEquals("v1", aVertices.iterator().next().getId());

        Iterable<Vertex> bVertices = graph.getVertices(AUTHORIZATIONS_B);
        assertEquals(1, count(bVertices));
        assertEquals("v2", bVertices.iterator().next().getId());

        Iterable<Vertex> allVertices = graph.getVertices(AUTHORIZATIONS_A_AND_B);
        assertEquals(2, count(allVertices));
    }

    @Test
    public void testRemoveVertex() {
        graph.addVertex("v1", VISIBILITY_A);

        assertEquals(1, count(graph.getVertices(AUTHORIZATIONS_A)));

        try {
            graph.removeVertex("v1", AUTHORIZATIONS_B);
        } catch (IllegalArgumentException e) {
            // expected
        }
        assertEquals(1, count(graph.getVertices(AUTHORIZATIONS_A)));

        graph.removeVertex("v1", AUTHORIZATIONS_A);
        assertEquals(0, count(graph.getVertices(AUTHORIZATIONS_A)));
    }

    @Test
    public void testRemoveVertexWithProperties() {
        graph.addVertex("v1", VISIBILITY_A,
                new Property("prop1", "value1", VISIBILITY_B));

        assertEquals(1, count(graph.getVertices(AUTHORIZATIONS_A)));

        try {
            graph.removeVertex("v1", AUTHORIZATIONS_B);
        } catch (IllegalArgumentException e) {
            // expected
        }
        assertEquals(1, count(graph.getVertices(AUTHORIZATIONS_A)));

        graph.removeVertex("v1", AUTHORIZATIONS_A);
        assertEquals(0, count(graph.getVertices(AUTHORIZATIONS_A_AND_B)));
    }

    @Test
    public void testAddEdge() {
        Vertex v1 = graph.addVertex("v1", VISIBILITY_A);
        Vertex v2 = graph.addVertex("v2", VISIBILITY_A);
        Edge e = graph.addEdge("e1", v1, v2, "label1", VISIBILITY_A);
        assertNotNull(e);
        assertEquals("e1", e.getId());
        assertEquals("label1", e.getLabel());
        assertEquals("v1", e.getVertexId(Direction.OUT));
        assertEquals(v1, e.getVertex(Direction.OUT, AUTHORIZATIONS_A));
        assertEquals("v2", e.getVertexId(Direction.IN));
        assertEquals(v2, e.getVertex(Direction.IN, AUTHORIZATIONS_A));
        assertEquals(VISIBILITY_A, e.getVisibility());

        e = graph.getEdge("e1", AUTHORIZATIONS_B);
        assertNull(e);

        e = graph.getEdge("e1", AUTHORIZATIONS_A);
        assertNotNull(e);
        assertEquals("e1", e.getId());
        assertEquals("label1", e.getLabel());
        assertEquals("v1", e.getVertexId(Direction.OUT));
        assertEquals(v1, e.getVertex(Direction.OUT, AUTHORIZATIONS_A));
        assertEquals("v2", e.getVertexId(Direction.IN));
        assertEquals(v2, e.getVertex(Direction.IN, AUTHORIZATIONS_A));
        assertEquals(VISIBILITY_A, e.getVisibility());
    }

    @Test
    public void testAddEdgeWithProperties() {
        Vertex v1 = graph.addVertex("v1", VISIBILITY_A);
        Vertex v2 = graph.addVertex("v2", VISIBILITY_A);
        graph.addEdge("e1", v1, v2, "label1", VISIBILITY_A,
                new Property("propA", "valueA", VISIBILITY_A),
                new Property("propB", "valueB", VISIBILITY_B));

        Edge e = graph.getEdge("e1", AUTHORIZATIONS_A);
        assertEquals(1, count(e.getProperties()));
        assertEquals("valueA", e.getPropertyValues("propA").iterator().next());
        assertEquals(0, count(e.getPropertyValues("propB")));

        e = graph.getEdge("e1", AUTHORIZATIONS_A_AND_B);
        assertEquals(2, count(e.getProperties()));
        assertEquals("valueA", e.getPropertyValues("propA").iterator().next());
        assertEquals("valueB", e.getPropertyValues("propB").iterator().next());
    }

    @Test
    public void testRemoveEdge() {
        Vertex v1 = graph.addVertex("v1", VISIBILITY_A);
        Vertex v2 = graph.addVertex("v2", VISIBILITY_A);
        graph.addEdge("e1", v1, v2, "label1", VISIBILITY_A);

        assertEquals(1, count(graph.getEdges(AUTHORIZATIONS_A)));

        try {
            graph.removeEdge("e1", AUTHORIZATIONS_B);
        } catch (IllegalArgumentException e) {
            // expected
        }
        assertEquals(1, count(graph.getEdges(AUTHORIZATIONS_A)));

        graph.removeEdge("e1", AUTHORIZATIONS_A);
        assertEquals(0, count(graph.getEdges(AUTHORIZATIONS_A)));
    }

    @Test
    public void testAddEdgeWithVisibility() {
        Vertex v1 = graph.addVertex("v1", VISIBILITY_A);
        Vertex v2 = graph.addVertex("v2", VISIBILITY_A);
        graph.addEdge("e1", v1, v2, "edgeA", VISIBILITY_A);
        graph.addEdge("e2", v1, v2, "edgeB", VISIBILITY_B);

        Iterable<Edge> aEdges = graph.getVertex("v1", AUTHORIZATIONS_A_AND_B).getEdges(Direction.BOTH, AUTHORIZATIONS_A);
        assertEquals(1, count(aEdges));
        Edge e1 = aEdges.iterator().next();
        assertNotNull(e1);
        assertEquals("edgeA", e1.getLabel());

        Iterable<Edge> bEdges = graph.getVertex("v1", AUTHORIZATIONS_A_AND_B).getEdges(Direction.BOTH, AUTHORIZATIONS_B);
        assertEquals(1, count(bEdges));
        Edge e2 = bEdges.iterator().next();
        assertNotNull(e2);
        assertEquals("edgeB", e2.getLabel());

        Iterable<Edge> allEdges = graph.getVertex("v1", AUTHORIZATIONS_A_AND_B).getEdges(Direction.BOTH, AUTHORIZATIONS_A_AND_B);
        assertEquals(2, count(allEdges));
    }

    @Test
    public void testGraphQuery() {
        Vertex v1 = graph.addVertex("v1", VISIBILITY_A);
        Vertex v2 = graph.addVertex("v2", VISIBILITY_A);
        graph.addEdge("e1", v1, v2, "edgeA", VISIBILITY_A);

        Iterable<Vertex> vertices = graph.query(AUTHORIZATIONS_A).vertices();
        assertEquals(2, count(vertices));

        vertices = graph.query(AUTHORIZATIONS_A).skip(1).vertices();
        assertEquals(1, count(vertices));

        vertices = graph.query(AUTHORIZATIONS_A).limit(1).vertices();
        assertEquals(1, count(vertices));

        vertices = graph.query(AUTHORIZATIONS_A).skip(1).limit(1).vertices();
        assertEquals(1, count(vertices));

        vertices = graph.query(AUTHORIZATIONS_A).skip(2).vertices();
        assertEquals(0, count(vertices));

        vertices = graph.query(AUTHORIZATIONS_A).skip(1).limit(2).vertices();
        assertEquals(1, count(vertices));

        Iterable<Edge> edges = graph.query(AUTHORIZATIONS_A).edges();
        assertEquals(1, count(edges));
    }

    @Test
    public void testGraphQueryWithQueryString() {
        Vertex v1 = graph.addVertex("v1", VISIBILITY_A);
        v1.setProperties(new Property("description", "This is vertex 1 - dog.", VISIBILITY_A));
        Vertex v2 = graph.addVertex("v2", VISIBILITY_A);
        v2.setProperties(new Property("description", "This is vertex 2 - cat.", VISIBILITY_A));

        Iterable<Vertex> vertices = graph.query("vertex", AUTHORIZATIONS_A).vertices();
        assertEquals(2, count(vertices));

        vertices = graph.query("dog", AUTHORIZATIONS_A).vertices();
        assertEquals(1, count(vertices));

        // TODO elastic search can't filter based on authorizations
//        vertices = graph.query("dog", AUTHORIZATIONS_B).vertices();
//        assertEquals(0, count(vertices));
    }

    @Test
    public void testGraphQueryHas() {
        graph.addVertex("v1", VISIBILITY_A,
                new Property("age", 25, VISIBILITY_A));
        graph.addVertex("v2", VISIBILITY_A,
                new Property("age", 30, VISIBILITY_A));

        Iterable<Vertex> vertices = graph.query(AUTHORIZATIONS_A)
                .has("age", Compare.EQUAL, 25)
                .vertices();
        assertEquals(1, count(vertices));

        vertices = graph.query(AUTHORIZATIONS_A)
                .has("age", 25)
                .vertices();
        assertEquals(1, count(vertices));

        vertices = graph.query(AUTHORIZATIONS_A)
                .has("age", Compare.GREATER_THAN_EQUAL, 25)
                .vertices();
        assertEquals(2, count(vertices));

        vertices = graph.query(AUTHORIZATIONS_A)
                .has("age", Compare.GREATER_THAN, 25)
                .vertices();
        assertEquals(1, count(vertices));

        vertices = graph.query(AUTHORIZATIONS_A)
                .has("age", Compare.LESS_THAN, 26)
                .vertices();
        assertEquals(1, count(vertices));

        vertices = graph.query(AUTHORIZATIONS_A)
                .has("age", Compare.LESS_THAN_EQUAL, 25)
                .vertices();
        assertEquals(1, count(vertices));

        vertices = graph.query(AUTHORIZATIONS_A)
                .has("age", Compare.NOT_EQUAL, 25)
                .vertices();
        assertEquals(1, count(vertices));
    }

    @Test
    public void testGraphQueryRange() {
        graph.addVertex("v1", VISIBILITY_A,
                new Property("age", 25, VISIBILITY_A));
        graph.addVertex("v2", VISIBILITY_A,
                new Property("age", 30, VISIBILITY_A));

        Iterable<Vertex> vertices = graph.query(AUTHORIZATIONS_A)
                .range("age", 25, 25)
                .vertices();
        assertEquals(1, count(vertices));

        vertices = graph.query(AUTHORIZATIONS_A)
                .range("age", 20, 29)
                .vertices();
        assertEquals(1, count(vertices));

        vertices = graph.query(AUTHORIZATIONS_A)
                .range("age", 25, 30)
                .vertices();
        assertEquals(2, count(vertices));
    }

    @Test
    public void testVertexQuery() {
        Vertex v1 = graph.addVertex("v1", VISIBILITY_A);
        Vertex v2 = graph.addVertex("v2", VISIBILITY_A);
        graph.addEdge("e1", v1, v2, "edgeA", VISIBILITY_A);

        v1 = graph.getVertex("v1", AUTHORIZATIONS_A);
        Iterable<Vertex> vertices = v1.query(AUTHORIZATIONS_A).vertices();
        assertEquals(1, count(vertices));
        assertEquals("v2", vertices.iterator().next().getId());

        Iterable<Edge> edges = v1.query(AUTHORIZATIONS_A).edges();
        assertEquals(1, count(edges));

        edges = v1.query(AUTHORIZATIONS_A).edges(Direction.OUT);
        assertEquals(1, count(edges));
    }
}
