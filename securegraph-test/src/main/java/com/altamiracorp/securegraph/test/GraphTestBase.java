package com.altamiracorp.securegraph.test;


import com.altamiracorp.securegraph.*;
import com.altamiracorp.securegraph.mutation.ElementMutation;
import com.altamiracorp.securegraph.property.PropertyValue;
import com.altamiracorp.securegraph.property.StreamingPropertyValue;
import com.altamiracorp.securegraph.query.Compare;
import com.altamiracorp.securegraph.query.DefaultGraphQuery;
import com.altamiracorp.securegraph.query.GeoCompare;
import com.altamiracorp.securegraph.query.TextPredicate;
import com.altamiracorp.securegraph.test.util.LargeStringInputStream;
import com.altamiracorp.securegraph.type.GeoCircle;
import com.altamiracorp.securegraph.type.GeoPoint;
import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.*;

import static com.altamiracorp.securegraph.test.util.IterableUtils.assertContains;
import static com.altamiracorp.securegraph.util.IterableUtils.count;
import static com.altamiracorp.securegraph.util.IterableUtils.toList;
import static org.junit.Assert.*;

@RunWith(JUnit4.class)
public abstract class GraphTestBase {
    public static final Visibility VISIBILITY_A = new Visibility("a");
    public static final Visibility VISIBILITY_B = new Visibility("b");
    public static final Visibility VISIBILITY_EMPTY = new Visibility("");
    public final Authorizations AUTHORIZATIONS_A;
    public final Authorizations AUTHORIZATIONS_B;
    public final Authorizations AUTHORIZATIONS_C;
    public final Authorizations AUTHORIZATIONS_A_AND_B;
    public final Authorizations AUTHORIZATIONS_EMPTY;
    public static final int LARGE_PROPERTY_VALUE_SIZE = 1024 + 1;

    protected Graph graph;

    protected abstract Graph createGraph() throws Exception;

    public Graph getGraph() {
        return graph;
    }

    public GraphTestBase() {
        AUTHORIZATIONS_A = createAuthorizations("a");
        AUTHORIZATIONS_B = createAuthorizations("b");
        AUTHORIZATIONS_C = createAuthorizations("c");
        AUTHORIZATIONS_A_AND_B = createAuthorizations("a", "b");
        AUTHORIZATIONS_EMPTY = createAuthorizations();
    }

    protected abstract Authorizations createAuthorizations(String... auths);

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
        Vertex v = graph.addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A);
        assertNotNull(v);
        assertEquals("v1", v.getId());

        v = graph.getVertex("v1", AUTHORIZATIONS_A);
        assertNotNull(v);
        assertEquals("v1", v.getId());
        assertEquals(VISIBILITY_A, v.getVisibility());

        v = graph.getVertex("", AUTHORIZATIONS_A);
        assertNull(v);

        v = graph.getVertex(null, AUTHORIZATIONS_A);
        assertNull(v);
    }

    @Test
    public void testAddVertexWithoutId() {
        Vertex v = graph.addVertex(VISIBILITY_A, AUTHORIZATIONS_A);
        assertNotNull(v);
        Object vertexId = v.getId();
        assertNotNull(vertexId);

        v = graph.getVertex(vertexId, AUTHORIZATIONS_A);
        assertNotNull(v);
        assertNotNull(vertexId);
    }

    @Test
    public void testAddStreamingPropertyValue() throws IOException, InterruptedException {
        String expectedLargeValue = IOUtils.toString(new LargeStringInputStream(LARGE_PROPERTY_VALUE_SIZE));
        PropertyValue propSmall = new StreamingPropertyValue(new ByteArrayInputStream("value1".getBytes()), String.class);
        PropertyValue propLarge = new StreamingPropertyValue(new ByteArrayInputStream(expectedLargeValue.getBytes()), String.class);
        String largePropertyName = "propLarge/\\*!@#$%^&*()[]{}|";
        Vertex v1 = graph.prepareVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A)
                .setProperty("propSmall", propSmall, VISIBILITY_A)
                .setProperty(largePropertyName, propLarge, VISIBILITY_A)
                .save();

        Iterable<Object> propSmallValues = v1.getPropertyValues("propSmall");
        assertEquals(1, count(propSmallValues));
        Object propSmallValue = propSmallValues.iterator().next();
        assertTrue("propSmallValue was " + propSmallValue.getClass().getName(), propSmallValue instanceof StreamingPropertyValue);
        StreamingPropertyValue value = (StreamingPropertyValue) propSmallValue;
        assertEquals(String.class, value.getValueType());
        assertEquals("value1".getBytes().length, value.getLength());
        assertEquals("value1", IOUtils.toString(value.getInputStream()));
        assertEquals("value1", IOUtils.toString(value.getInputStream()));

        Iterable<Object> propLargeValues = v1.getPropertyValues(largePropertyName);
        assertEquals(1, count(propLargeValues));
        Object propLargeValue = propLargeValues.iterator().next();
        assertTrue(largePropertyName + " was " + propLargeValue.getClass().getName(), propLargeValue instanceof StreamingPropertyValue);
        value = (StreamingPropertyValue) propLargeValue;
        assertEquals(String.class, value.getValueType());
        assertEquals(expectedLargeValue.getBytes().length, value.getLength());
        assertEquals(expectedLargeValue, IOUtils.toString(value.getInputStream()));
        assertEquals(expectedLargeValue, IOUtils.toString(value.getInputStream()));

        v1 = graph.getVertex("v1", AUTHORIZATIONS_A);
        propSmallValues = v1.getPropertyValues("propSmall");
        assertEquals(1, count(propSmallValues));
        propSmallValue = propSmallValues.iterator().next();
        assertTrue("propSmallValue was " + propSmallValue.getClass().getName(), propSmallValue instanceof StreamingPropertyValue);
        value = (StreamingPropertyValue) propSmallValue;
        assertEquals(String.class, value.getValueType());
        assertEquals("value1".getBytes().length, value.getLength());
        assertEquals("value1", IOUtils.toString(value.getInputStream()));
        assertEquals("value1", IOUtils.toString(value.getInputStream()));

        propLargeValues = v1.getPropertyValues(largePropertyName);
        assertEquals(1, count(propLargeValues));
        propLargeValue = propLargeValues.iterator().next();
        assertTrue(largePropertyName + " was " + propLargeValue.getClass().getName(), propLargeValue instanceof StreamingPropertyValue);
        value = (StreamingPropertyValue) propLargeValue;
        assertEquals(String.class, value.getValueType());
        assertEquals(expectedLargeValue.getBytes().length, value.getLength());
        assertEquals(expectedLargeValue, IOUtils.toString(value.getInputStream()));
        assertEquals(expectedLargeValue, IOUtils.toString(value.getInputStream()));
    }

    @Test
    public void testAddVertexPropertyWithMetadata() {
        Map<String, Object> prop1Metadata = new HashMap<String, Object>();
        prop1Metadata.put("metadata1", "metadata1Value");

        graph.prepareVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A)
                .setProperty("prop1", "value1", prop1Metadata, VISIBILITY_A)
                .save();

        Vertex v = graph.getVertex("v1", AUTHORIZATIONS_A);
        assertEquals(1, count(v.getProperties("prop1")));
        Property prop1 = v.getProperties("prop1").iterator().next();
        prop1Metadata = prop1.getMetadata();
        assertNotNull(prop1Metadata);
        assertEquals(1, prop1Metadata.keySet().size());
        assertEquals("metadata1Value", prop1Metadata.get("metadata1"));

        prop1Metadata.put("metadata2", "metadata2Value");
        v.prepareMutation()
                .setProperty("prop1", "value1", prop1Metadata, VISIBILITY_A)
                .save();

        v = graph.getVertex("v1", AUTHORIZATIONS_A);
        assertEquals(1, count(v.getProperties("prop1")));
        prop1 = v.getProperties("prop1").iterator().next();
        prop1Metadata = prop1.getMetadata();
        assertEquals(2, prop1Metadata.keySet().size());
        assertEquals("metadata1Value", prop1Metadata.get("metadata1"));
        assertEquals("metadata2Value", prop1Metadata.get("metadata2"));

        // make sure we clear out old values
        prop1Metadata = new HashMap<String, Object>();
        v.setProperty("prop1", "value1", prop1Metadata, VISIBILITY_A);

        v = graph.getVertex("v1", AUTHORIZATIONS_A);
        assertEquals(1, count(v.getProperties("prop1")));
        prop1 = v.getProperties("prop1").iterator().next();
        prop1Metadata = prop1.getMetadata();
        assertEquals(0, prop1Metadata.keySet().size());
    }

    @Test
    public void testAddVertexWithProperties() {
        Vertex v = graph.prepareVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A)
                .setProperty("prop1", "value1", VISIBILITY_A)
                .setProperty("prop2", "value2", VISIBILITY_B)
                .save();
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
    public void testAddVertexWithPropertiesWithTwoDifferentVisibilities() {
        Vertex v = graph.prepareVertex("v1", VISIBILITY_EMPTY, AUTHORIZATIONS_A_AND_B)
                .setProperty("prop1", "value1a", VISIBILITY_A)
                .setProperty("prop1", "value1b", VISIBILITY_B)
                .save();
        assertEquals(2, count(v.getProperties("prop1")));

        v = graph.getVertex("v1", AUTHORIZATIONS_A_AND_B);
        assertEquals(2, count(v.getProperties("prop1")));

        v = graph.getVertex("v1", AUTHORIZATIONS_A);
        assertEquals(1, count(v.getProperties("prop1")));
        assertEquals("value1a", v.getPropertyValue("prop1"));

        v = graph.getVertex("v1", AUTHORIZATIONS_B);
        assertEquals(1, count(v.getProperties("prop1")));
        assertEquals("value1b", v.getPropertyValue("prop1"));
    }

    @Test
    public void testMultivaluedProperties() {
        Vertex v = graph.addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A);

        v.prepareMutation()
                .addPropertyValue("propid1a", "prop1", "value1a", VISIBILITY_A)
                .addPropertyValue("propid2a", "prop2", "value2a", VISIBILITY_A)
                .addPropertyValue("propid3a", "prop3", "value3a", VISIBILITY_A)
                .save();
        v = graph.getVertex("v1", AUTHORIZATIONS_A);
        assertEquals("value1a", v.getPropertyValues("prop1").iterator().next());
        assertEquals("value2a", v.getPropertyValues("prop2").iterator().next());
        assertEquals("value3a", v.getPropertyValues("prop3").iterator().next());
        assertEquals(3, count(v.getProperties()));

        v.prepareMutation()
                .addPropertyValue("propid1a", "prop1", "value1b", VISIBILITY_A)
                .addPropertyValue("propid2a", "prop2", "value2b", VISIBILITY_A)
                .save();
        v = graph.getVertex("v1", AUTHORIZATIONS_A);
        assertEquals(1, count(v.getPropertyValues("prop1")));
        assertEquals("value1b", v.getPropertyValues("prop1").iterator().next());
        assertEquals(1, count(v.getPropertyValues("prop2")));
        assertEquals("value2b", v.getPropertyValues("prop2").iterator().next());
        assertEquals(1, count(v.getPropertyValues("prop3")));
        assertEquals("value3a", v.getPropertyValues("prop3").iterator().next());
        assertEquals(3, count(v.getProperties()));

        v.addPropertyValue("propid1b", "prop1", "value1a-new", VISIBILITY_A);
        v = graph.getVertex("v1", AUTHORIZATIONS_A);
        assertContains("value1b", v.getPropertyValues("prop1"));
        assertContains("value1a-new", v.getPropertyValues("prop1"));
        assertEquals(4, count(v.getProperties()));
    }

    @Test
    public void testMultivaluedPropertyOrder() {
        graph.prepareVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A)
                .addPropertyValue("a", "prop", "a", VISIBILITY_A)
                .addPropertyValue("aa", "prop", "aa", VISIBILITY_A)
                .addPropertyValue("b", "prop", "b", VISIBILITY_A)
                .addPropertyValue("0", "prop", "0", VISIBILITY_A)
                .addPropertyValue("A", "prop", "A", VISIBILITY_A)
                .addPropertyValue("Z", "prop", "Z", VISIBILITY_A)
                .save();

        Vertex v1 = graph.getVertex("v1", AUTHORIZATIONS_A);
        assertEquals("0", v1.getPropertyValue("prop", 0));
        assertEquals("A", v1.getPropertyValue("prop", 1));
        assertEquals("Z", v1.getPropertyValue("prop", 2));
        assertEquals("a", v1.getPropertyValue("prop", 3));
        assertEquals("aa", v1.getPropertyValue("prop", 4));
        assertEquals("b", v1.getPropertyValue("prop", 5));
    }

    @Test
    public void testRemoveProperty() {
        Vertex v = graph.addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A);

        v.prepareMutation()
                .addPropertyValue("propid1a", "prop1", "value1a", VISIBILITY_A)
                .addPropertyValue("propid1b", "prop1", "value1b", VISIBILITY_A)
                .addPropertyValue("propid2a", "prop2", "value2a", VISIBILITY_A)
                .save();

        v = graph.getVertex("v1", AUTHORIZATIONS_A);
        v.removeProperty("prop1");
        assertEquals(1, count(v.getProperties()));
        v = graph.getVertex("v1", AUTHORIZATIONS_A);
        assertEquals(1, count(v.getProperties()));

        v.removeProperty("propid2a", "prop2");
        assertEquals(0, count(v.getProperties()));
        v = graph.getVertex("v1", AUTHORIZATIONS_A);
        assertEquals(0, count(v.getProperties()));
    }

    @Test
    public void testAddVertexWithVisibility() {
        graph.addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A);
        graph.addVertex("v2", VISIBILITY_B, AUTHORIZATIONS_A);

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
    public void testGetVerticesWithIds() {
        graph.prepareVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A)
                .setProperty("prop1", "v1", VISIBILITY_A)
                .save();
        graph.prepareVertex("v1b", VISIBILITY_A, AUTHORIZATIONS_A)
                .setProperty("prop1", "v1b", VISIBILITY_A)
                .save();
        graph.prepareVertex("v2", VISIBILITY_A, AUTHORIZATIONS_A)
                .setProperty("prop1", "v2", VISIBILITY_A)
                .save();
        graph.prepareVertex("v3", VISIBILITY_A, AUTHORIZATIONS_A)
                .setProperty("prop1", "v3", VISIBILITY_A)
                .save();

        List<Object> ids = new ArrayList<Object>();
        ids.add("v2");
        ids.add("v1");

        Iterable<Vertex> vertices = graph.getVertices(ids, AUTHORIZATIONS_A);
        boolean foundV1 = false, foundV2 = false;
        for (Vertex v : vertices) {
            if (v.getId().equals("v1")) {
                assertEquals("v1", v.getPropertyValue("prop1"));
                foundV1 = true;
            } else if (v.getId().equals("v2")) {
                assertEquals("v2", v.getPropertyValue("prop1"));
                foundV2 = true;
            } else {
                assertTrue("Unexpected vertex id: " + v.getId(), false);
            }
        }
        assertTrue("v1 not found", foundV1);
        assertTrue("v2 not found", foundV2);

        List<Vertex> verticesInOrder = graph.getVerticesInOrder(ids, AUTHORIZATIONS_A);
        assertEquals(2, verticesInOrder.size());
        assertEquals("v2", verticesInOrder.get(0).getId());
        assertEquals("v1", verticesInOrder.get(1).getId());
    }

    @Test
    public void testGetEdgesWithIds() {
        Vertex v1 = graph.addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A);
        Vertex v2 = graph.addVertex("v2", VISIBILITY_A, AUTHORIZATIONS_A);
        Vertex v3 = graph.addVertex("v3", VISIBILITY_A, AUTHORIZATIONS_A);
        graph.prepareEdge("e1", v1, v2, "", VISIBILITY_A, AUTHORIZATIONS_A)
                .setProperty("prop1", "e1", VISIBILITY_A)
                .save();
        graph.prepareEdge("e1a", v1, v2, "", VISIBILITY_A, AUTHORIZATIONS_A)
                .setProperty("prop1", "e1a", VISIBILITY_A)
                .save();
        graph.prepareEdge("e2", v1, v3, "", VISIBILITY_A, AUTHORIZATIONS_A)
                .setProperty("prop1", "e2", VISIBILITY_A)
                .save();
        graph.prepareEdge("e3", v2, v3, "", VISIBILITY_A, AUTHORIZATIONS_A)
                .setProperty("prop1", "e3", VISIBILITY_A)
                .save();

        List<Object> ids = new ArrayList<Object>();
        ids.add("e1");
        ids.add("e2");
        Iterable<Edge> edges = graph.getEdges(ids, AUTHORIZATIONS_A);
        boolean foundE1 = false, foundE2 = false;
        for (Edge e : edges) {
            if (e.getId().equals("e1")) {
                assertEquals("e1", e.getPropertyValue("prop1"));
                foundE1 = true;
            } else if (e.getId().equals("e2")) {
                assertEquals("e2", e.getPropertyValue("prop1"));
                foundE2 = true;
            } else {
                assertTrue("Unexpected vertex id: " + e.getId(), false);
            }
        }
        assertTrue("e1 not found", foundE1);
        assertTrue("e2 not found", foundE2);
    }

    @Test
    public void testRemoveVertex() {
        Vertex v1 = graph.addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A);

        assertEquals(1, count(graph.getVertices(AUTHORIZATIONS_A)));

        graph.removeVertex(v1, AUTHORIZATIONS_A);
        assertEquals(0, count(graph.getVertices(AUTHORIZATIONS_A)));
    }

    @Test
    public void testRemoveVertexWithProperties() {
        Vertex v1 = graph.prepareVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A)
                .setProperty("prop1", "value1", VISIBILITY_B)
                .save();

        assertEquals(1, count(graph.getVertices(AUTHORIZATIONS_A)));

        graph.removeVertex(v1, AUTHORIZATIONS_A);
        assertEquals(0, count(graph.getVertices(AUTHORIZATIONS_A_AND_B)));
    }

    @Test
    public void testAddEdge() {
        Vertex v1 = graph.addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A);
        Vertex v2 = graph.addVertex("v2", VISIBILITY_A, AUTHORIZATIONS_A);
        Edge e = graph.addEdge("e1", v1, v2, "label1", VISIBILITY_A, AUTHORIZATIONS_A);
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
    public void testGetEdge() {
        Vertex v1 = graph.addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A);
        Vertex v2 = graph.addVertex("v2", VISIBILITY_A, AUTHORIZATIONS_A);
        graph.addEdge("e1to2label1", v1, v2, "label1", VISIBILITY_A, AUTHORIZATIONS_A);
        graph.addEdge("e1to2label2", v1, v2, "label2", VISIBILITY_A, AUTHORIZATIONS_A);
        graph.addEdge("e2to1", v2, v1, "label1", VISIBILITY_A, AUTHORIZATIONS_A);

        v1 = graph.getVertex("v1", AUTHORIZATIONS_A);

        assertEquals(3, count(v1.getEdges(Direction.BOTH, AUTHORIZATIONS_A)));
        assertEquals(2, count(v1.getEdges(Direction.OUT, AUTHORIZATIONS_A)));
        assertEquals(1, count(v1.getEdges(Direction.IN, AUTHORIZATIONS_A)));
        assertEquals(3, count(v1.getEdges(v2, Direction.BOTH, AUTHORIZATIONS_A)));
        assertEquals(2, count(v1.getEdges(v2, Direction.OUT, AUTHORIZATIONS_A)));
        assertEquals(1, count(v1.getEdges(v2, Direction.IN, AUTHORIZATIONS_A)));
        assertEquals(2, count(v1.getEdges(v2, Direction.BOTH, "label1", AUTHORIZATIONS_A)));
        assertEquals(1, count(v1.getEdges(v2, Direction.OUT, "label1", AUTHORIZATIONS_A)));
        assertEquals(1, count(v1.getEdges(v2, Direction.IN, "label1", AUTHORIZATIONS_A)));
        assertEquals(3, count(v1.getEdges(v2, Direction.BOTH, new String[]{"label1", "label2"}, AUTHORIZATIONS_A)));
        assertEquals(2, count(v1.getEdges(v2, Direction.OUT, new String[]{"label1", "label2"}, AUTHORIZATIONS_A)));
        assertEquals(1, count(v1.getEdges(v2, Direction.IN, new String[]{"label1", "label2"}, AUTHORIZATIONS_A)));
    }

    @Test
    public void testAddEdgeWithProperties() {
        Vertex v1 = graph.addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A);
        Vertex v2 = graph.addVertex("v2", VISIBILITY_A, AUTHORIZATIONS_A);
        graph.prepareEdge("e1", v1, v2, "label1", VISIBILITY_A, AUTHORIZATIONS_A)
                .setProperty("propA", "valueA", VISIBILITY_A)
                .setProperty("propB", "valueB", VISIBILITY_B)
                .save();

        Edge e = graph.getEdge("e1", AUTHORIZATIONS_A);
        assertEquals(1, count(e.getProperties()));
        assertEquals("valueA", e.getPropertyValues("propA").iterator().next());
        assertEquals(0, count(e.getPropertyValues("propB")));

        e = graph.getEdge("e1", AUTHORIZATIONS_A_AND_B);
        assertEquals(2, count(e.getProperties()));
        assertEquals("valueA", e.getPropertyValues("propA").iterator().next());
        assertEquals("valueB", e.getPropertyValues("propB").iterator().next());
        assertEquals("valueA", e.getPropertyValue("propA"));
        assertEquals("valueB", e.getPropertyValue("propB"));
    }

    @Test
    public void testRemoveEdge() {
        Vertex v1 = graph.addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A);
        Vertex v2 = graph.addVertex("v2", VISIBILITY_A, AUTHORIZATIONS_A);
        graph.addEdge("e1", v1, v2, "label1", VISIBILITY_A, AUTHORIZATIONS_A);

        assertEquals(1, count(graph.getEdges(AUTHORIZATIONS_A)));

        try {
            graph.removeEdge("e1", AUTHORIZATIONS_B);
        } catch (IllegalArgumentException e) {
            // expected
        }
        assertEquals(1, count(graph.getEdges(AUTHORIZATIONS_A)));

        graph.removeEdge("e1", AUTHORIZATIONS_A);
        assertEquals(0, count(graph.getEdges(AUTHORIZATIONS_A)));

        v1 = graph.getVertex("v1", AUTHORIZATIONS_A);
        assertEquals(0, count(v1.getVertices(Direction.BOTH, AUTHORIZATIONS_A)));
        v2 = graph.getVertex("v2", AUTHORIZATIONS_A);
        assertEquals(0, count(v2.getVertices(Direction.BOTH, AUTHORIZATIONS_A)));
    }

    @Test
    public void testAddEdgeWithVisibility() {
        Vertex v1 = graph.addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A);
        Vertex v2 = graph.addVertex("v2", VISIBILITY_A, AUTHORIZATIONS_A);
        graph.addEdge("e1", v1, v2, "edgeA", VISIBILITY_A, AUTHORIZATIONS_A);
        graph.addEdge("e2", v1, v2, "edgeB", VISIBILITY_B, AUTHORIZATIONS_B);

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
        Vertex v1 = graph.addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A);
        Vertex v2 = graph.addVertex("v2", VISIBILITY_A, AUTHORIZATIONS_A);
        graph.addEdge("e1", v1, v2, "edgeA", VISIBILITY_A, AUTHORIZATIONS_A);

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
        Vertex v1 = graph.addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A);
        v1.setProperty("description", "This is vertex 1 - dog.", VISIBILITY_A);
        Vertex v2 = graph.addVertex("v2", VISIBILITY_A, AUTHORIZATIONS_A);
        v2.setProperty("description", "This is vertex 2 - cat.", VISIBILITY_A);

        Iterable<Vertex> vertices = graph.query("vertex", AUTHORIZATIONS_A).vertices();
        assertEquals(2, count(vertices));

        vertices = graph.query("dog", AUTHORIZATIONS_A).vertices();
        assertEquals(1, count(vertices));

        vertices = graph.query("dog", AUTHORIZATIONS_B).vertices();
        assertEquals(0, count(vertices));
    }

    @Test
    public void testGraphQueryHas() {
        graph.prepareVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A)
                .setProperty("age", 25, VISIBILITY_A)
                .setProperty("birthDate", new DateOnly(1989, 1, 5), VISIBILITY_A)
                .setProperty("lastAccessed", createDate(2014, 2, 24, 13, 0, 5), VISIBILITY_A)
                .save();
        graph.prepareVertex("v2", VISIBILITY_A, AUTHORIZATIONS_A)
                .setProperty("age", 30, VISIBILITY_A)
                .setProperty("birthDate", new DateOnly(1984, 1, 5), VISIBILITY_A)
                .setProperty("lastAccessed", createDate(2014, 2, 25, 13, 0, 5), VISIBILITY_A)
                .save();

        Iterable<Vertex> vertices = graph.query(AUTHORIZATIONS_A)
                .has("age", Compare.EQUAL, 25)
                .vertices();
        assertEquals(1, count(vertices));

        vertices = graph.query(AUTHORIZATIONS_A)
                .has("birthDate", Compare.EQUAL, createDate(1989, 1, 5))
                .vertices();
        assertEquals(1, count(vertices));

        vertices = graph.query(AUTHORIZATIONS_A)
                .has("lastAccessed", Compare.EQUAL, createDate(2014, 2, 24, 13, 0, 5))
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
                .has("age", Compare.IN, new Integer[]{25})
                .vertices();
        assertEquals(1, count(vertices));

        vertices = graph.query(AUTHORIZATIONS_A)
                .has("age", Compare.IN, new Integer[]{25, 30})
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

        vertices = graph.query(AUTHORIZATIONS_A)
                .has("lastAccessed", Compare.EQUAL, new DateOnly(2014, 2, 24))
                .vertices();
        assertEquals(1, count(vertices));

        vertices = graph.query("*", AUTHORIZATIONS_A)
                .has("age", Compare.IN, new Integer[]{25, 30})
                .vertices();
        assertEquals(2, count(vertices));
    }

    @Test
    public void testGraphQueryVertexHasWithSecurity() {
        graph.prepareVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A)
                .setProperty("age", 25, VISIBILITY_A)
                .save();
        graph.prepareVertex("v2", VISIBILITY_A, AUTHORIZATIONS_A)
                .setProperty("age", 25, VISIBILITY_B)
                .save();

        Iterable<Vertex> vertices = graph.query(AUTHORIZATIONS_A)
                .has("age", Compare.EQUAL, 25)
                .vertices();
        assertEquals(1, count(vertices));
    }

    @Test
    public void testGraphQueryEdgeHasWithSecurity() {
        Vertex v1 = graph.prepareVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A).save();
        Vertex v2 = graph.prepareVertex("v2", VISIBILITY_A, AUTHORIZATIONS_A).save();
        Vertex v3 = graph.prepareVertex("v3", VISIBILITY_A, AUTHORIZATIONS_A).save();

        graph.prepareEdge("e1", v1, v2, "edge", VISIBILITY_A, AUTHORIZATIONS_A)
                .setProperty("age", 25, VISIBILITY_A)
                .save();
        graph.prepareEdge("e2", v1, v3, "edge", VISIBILITY_A, AUTHORIZATIONS_A)
                .setProperty("age", 25, VISIBILITY_B)
                .save();

        Iterable<Edge> edges = graph.query(AUTHORIZATIONS_A)
                .has("age", Compare.EQUAL, 25)
                .edges();
        assertEquals(1, count(edges));
    }

    @Test
    public void testGraphQueryHasWithSpaces() {
        graph.prepareVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A)
                .setProperty("name", "Joe Ferner", VISIBILITY_A)
                .setProperty("propWithHyphen", "hyphen-word", VISIBILITY_A)
                .save();
        graph.prepareVertex("v2", VISIBILITY_A, AUTHORIZATIONS_A)
                .setProperty("name", "Joe Smith", VISIBILITY_A)
                .save();

        Iterable<Vertex> vertices = graph.query("Ferner", AUTHORIZATIONS_A)
                .vertices();
        assertEquals(1, count(vertices));

        vertices = graph.query("joe", AUTHORIZATIONS_A)
                .vertices();
        assertEquals(2, count(vertices));

        if (!isUsingDefaultQuery(graph)) {
            vertices = graph.query("joe AND ferner", AUTHORIZATIONS_A)
                    .vertices();
            assertEquals(1, count(vertices));
        }

        if (!isUsingDefaultQuery(graph)) {
            vertices = graph.query("name:\"joe ferner\"", AUTHORIZATIONS_A)
                    .vertices();
            assertEquals(1, count(vertices));
        }

        if (!isUsingDefaultQuery(graph)) {
            vertices = graph.query("joe smith", AUTHORIZATIONS_A)
                    .vertices();
            List<Vertex> verticesList = toList(vertices);
            assertEquals(2, verticesList.size());
            assertEquals("v2", verticesList.get(0).getId());
            assertEquals("v1", verticesList.get(1).getId());
        }

        vertices = graph.query(AUTHORIZATIONS_A)
                .has("name", TextPredicate.CONTAINS, "Ferner")
                .vertices();
        assertEquals(1, count(vertices));

        vertices = graph.query(AUTHORIZATIONS_A)
                .has("name", TextPredicate.CONTAINS, "Joe")
                .has("name", TextPredicate.CONTAINS, "Ferner")
                .vertices();
        assertEquals(1, count(vertices));

        vertices = graph.query(AUTHORIZATIONS_A)
                .has("name", TextPredicate.CONTAINS, "Joe Ferner")
                .vertices();
        assertEquals(1, count(vertices));

        vertices = graph.query(AUTHORIZATIONS_A)
                .has("propWithHyphen", TextPredicate.CONTAINS, "hyphen-word")
                .vertices();
        assertEquals(1, count(vertices));
    }

    protected boolean isUsingDefaultQuery(Graph graph) {
        return graph.query(AUTHORIZATIONS_A) instanceof DefaultGraphQuery;
    }

    @Test
    public void testGraphQueryGeoPoint() {
        graph.prepareVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A)
                .setProperty("location", new GeoPoint(38.9186, -77.2297, "Reston, VA"), VISIBILITY_A)
                .save();
        graph.prepareVertex("v2", VISIBILITY_A, AUTHORIZATIONS_A)
                .setProperty("location", new GeoPoint(38.9544, -77.3464, "Reston, VA"), VISIBILITY_A)
                .save();

        List<Vertex> vertices = toList(graph.query(AUTHORIZATIONS_A)
                .has("location", GeoCompare.WITHIN, new GeoCircle(38.9186, -77.2297, 1))
                .vertices());
        assertEquals(1, count(vertices));
        GeoPoint geoPoint = (GeoPoint) vertices.get(0).getPropertyValue("location");
        assertEquals(38.9186, geoPoint.getLatitude(), 0.001);
        assertEquals(-77.2297, geoPoint.getLongitude(), 0.001);
        assertEquals("Reston, VA", geoPoint.getDescription());

        vertices = toList(graph.query(AUTHORIZATIONS_A)
                .has("location", GeoCompare.WITHIN, new GeoCircle(38.9186, -77.2297, 15))
                .vertices());
        assertEquals(2, count(vertices));
    }

    private Date createDate(int year, int month, int day) {
        return new GregorianCalendar(year, month, day).getTime();
    }

    private Date createDate(int year, int month, int day, int hour, int min, int sec) {
        return new GregorianCalendar(year, month, day, hour, min, sec).getTime();
    }

    @Test
    public void testGraphQueryRange() {
        graph.prepareVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A)
                .setProperty("age", 25, VISIBILITY_A)
                .save();
        graph.prepareVertex("v2", VISIBILITY_A, AUTHORIZATIONS_A)
                .setProperty("age", 30, VISIBILITY_A)
                .save();

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
        Vertex v1 = graph.addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A);
        v1.setProperty("prop1", "value1", VISIBILITY_A);

        Vertex v2 = graph.addVertex("v2", VISIBILITY_A, AUTHORIZATIONS_A);
        v2.setProperty("prop1", "value2", VISIBILITY_A);

        Vertex v3 = graph.addVertex("v3", VISIBILITY_A, AUTHORIZATIONS_A);
        v3.setProperty("prop1", "value3", VISIBILITY_A);

        Edge ev1v2 = graph.addEdge("e v1->v2", v1, v2, "edgeA", VISIBILITY_A, AUTHORIZATIONS_A);
        Edge ev1v3 = graph.addEdge("e v1->v3", v1, v3, "edgeA", VISIBILITY_A, AUTHORIZATIONS_A);

        v1 = graph.getVertex("v1", AUTHORIZATIONS_A);
        Iterable<Vertex> vertices = v1.query(AUTHORIZATIONS_A).vertices();
        assertEquals(2, count(vertices));
        assertContains(v2, vertices);
        assertContains(v3, vertices);

        vertices = v1.query(AUTHORIZATIONS_A)
                .has("prop1", "value2")
                .vertices();
        assertEquals(1, count(vertices));
        assertContains(v2, vertices);

        Iterable<Edge> edges = v1.query(AUTHORIZATIONS_A).edges();
        assertEquals(2, count(edges));
        assertContains(ev1v2, edges);
        assertContains(ev1v3, edges);

        edges = v1.query(AUTHORIZATIONS_A).edges(Direction.OUT);
        assertEquals(2, count(edges));
        assertContains(ev1v2, edges);
        assertContains(ev1v3, edges);
    }

    @Test
    public void testFindPaths() {
        Vertex v1 = graph.addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A);
        Vertex v2 = graph.addVertex("v2", VISIBILITY_A, AUTHORIZATIONS_A);
        Vertex v3 = graph.addVertex("v3", VISIBILITY_A, AUTHORIZATIONS_A);
        Vertex v4 = graph.addVertex("v4", VISIBILITY_A, AUTHORIZATIONS_A);
        graph.addEdge(v1, v2, "knows", VISIBILITY_A, AUTHORIZATIONS_A); // v1 -> v2
        graph.addEdge(v2, v4, "knows", VISIBILITY_A, AUTHORIZATIONS_A); // v2 -> v4
        graph.addEdge(v1, v3, "knows", VISIBILITY_A, AUTHORIZATIONS_A); // v1 -> v3
        graph.addEdge(v3, v4, "knows", VISIBILITY_A, AUTHORIZATIONS_A); // v3 -> v4

        v1 = graph.getVertex("v1", AUTHORIZATIONS_A);
        v4 = graph.getVertex("v4", AUTHORIZATIONS_A);
        List<Path> paths = toList(graph.findPaths(v1, v4, 2, AUTHORIZATIONS_A));
        // v1 -> v2 -> v4
        // v1 -> v3 -> v4
        assertEquals(2, paths.size());
        boolean found2 = false;
        boolean found3 = false;
        for (Path path : paths) {
            assertEquals(3, path.length());
            int i = 0;
            for (Object id : path) {
                if (i == 0) {
                    assertEquals(id, v1.getId());
                } else if (i == 1) {
                    if (v2.getId().equals(id)) {
                        found2 = true;
                    } else if (v3.getId().equals(id)) {
                        found3 = true;
                    } else {
                        fail("center of path is neither v2 or v3 but found " + id);
                    }
                } else if (i == 2) {
                    assertEquals(id, v4.getId());
                }
                i++;
            }
        }
        assertTrue("v2 not found in path", found2);
        assertTrue("v3 not found in path", found3);

        v4 = graph.getVertex("v4", AUTHORIZATIONS_A);
        v1 = graph.getVertex("v1", AUTHORIZATIONS_A);
        paths = toList(graph.findPaths(v4, v1, 2, AUTHORIZATIONS_A));
        // v4 -> v2 -> v1
        // v4 -> v3 -> v1
        assertEquals(2, paths.size());
        found2 = false;
        found3 = false;
        for (Path path : paths) {
            assertEquals(3, path.length());
            int i = 0;
            for (Object id : path) {
                if (i == 0) {
                    assertEquals(id, v4.getId());
                } else if (i == 1) {
                    if (v2.getId().equals(id)) {
                        found2 = true;
                    } else if (v3.getId().equals(id)) {
                        found3 = true;
                    } else {
                        fail("center of path is neither v2 or v3 but found " + id);
                    }
                } else if (i == 2) {
                    assertEquals(id, v1.getId());
                }
                i++;
            }
        }
        assertTrue("v2 not found in path", found2);
        assertTrue("v3 not found in path", found3);
    }

    @Test
    public void testFindPathsMultiplePaths() {
        Vertex v1 = graph.addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A);
        Vertex v2 = graph.addVertex("v2", VISIBILITY_A, AUTHORIZATIONS_A);
        Vertex v3 = graph.addVertex("v3", VISIBILITY_A, AUTHORIZATIONS_A);
        Vertex v4 = graph.addVertex("v4", VISIBILITY_A, AUTHORIZATIONS_A);
        Vertex v5 = graph.addVertex("v5", VISIBILITY_A, AUTHORIZATIONS_A);

        graph.addEdge(v1, v4, "knows", VISIBILITY_A, AUTHORIZATIONS_A); // v1 -> v4
        graph.addEdge(v1, v3, "knows", VISIBILITY_A, AUTHORIZATIONS_A); // v1 -> v3
        graph.addEdge(v3, v4, "knows", VISIBILITY_A, AUTHORIZATIONS_A); // v3 -> v4
        graph.addEdge(v2, v3, "knows", VISIBILITY_A, AUTHORIZATIONS_A); // v2 -> v3
        graph.addEdge(v4, v2, "knows", VISIBILITY_A, AUTHORIZATIONS_A); // v4 -> v2
        graph.addEdge(v2, v5, "knows", VISIBILITY_A, AUTHORIZATIONS_A); // v2 -> v5

        v1 = graph.getVertex("v1", AUTHORIZATIONS_A);
        v2 = graph.getVertex("v2", AUTHORIZATIONS_A);
        v5 = graph.getVertex("v5", AUTHORIZATIONS_A);

        List<Path> paths = toList(graph.findPaths(v1, v2, 2, AUTHORIZATIONS_A));
        // v1 -> v4 -> v2
        // v1 -> v3 -> v2
        assertEquals(2, paths.size());
        boolean found3 = false;
        boolean found4 = false;
        for (Path path : paths) {
            assertEquals(3, path.length());
            int i = 0;
            for (Object id : path) {
                if (i == 0) {
                    assertEquals(id, v1.getId());
                } else if (i == 1) {
                    if (v3.getId().equals(id)) {
                        found3 = true;
                    } else if (v4.getId().equals(id)) {
                        found4 = true;
                    } else {
                        fail("center of path is neither v2 or v3 but found " + id);
                    }
                } else if (i == 2) {
                    assertEquals(id, v2.getId());
                }
                i++;
            }
        }
        assertTrue("v3 not found in path", found3);
        assertTrue("v4 not found in path", found4);

        paths = toList(graph.findPaths(v1, v2, 3, AUTHORIZATIONS_A));
        // v1 -> v4 -> v2
        // v1 -> v3 -> v2
        // v1 -> v3 -> v4 -> v2
        // v1 -> v4 -> v3 -> v2
        assertEquals(4, paths.size());
        found3 = false;
        found4 = false;
        for (Path path : paths) {
            if (path.length() == 3) {
                int i = 0;
                for (Object id : path) {
                    if (i == 0) {
                        assertEquals(id, v1.getId());
                    } else if (i == 1) {
                        if (v3.getId().equals(id)) {
                            found3 = true;
                        } else if (v4.getId().equals(id)) {
                            found4 = true;
                        } else {
                            fail("center of path is neither v2 or v3 but found " + id);
                        }
                    } else if (i == 2) {
                        assertEquals(id, v2.getId());
                    }
                    i++;
                }
            } else if (path.length() == 4) {

            } else {
                fail("Invalid path length " + path.length());
            }
        }
        assertTrue("v3 not found in path", found3);
        assertTrue("v4 not found in path", found4);

        paths = toList(graph.findPaths(v1, v5, 2, AUTHORIZATIONS_A));
        assertEquals(0, paths.size());

        paths = toList(graph.findPaths(v1, v5, 3, AUTHORIZATIONS_A));
        // v1 -> v4 -> v2 -> v5
        // v1 -> v3 -> v2 -> v5
        assertEquals(2, paths.size());
        found3 = false;
        found4 = false;
        for (Path path : paths) {
            assertEquals(4, path.length());
            int i = 0;
            for (Object id : path) {
                if (i == 0) {
                    assertEquals(id, v1.getId());
                } else if (i == 1) {
                    if (v3.getId().equals(id)) {
                        found3 = true;
                    } else if (v4.getId().equals(id)) {
                        found4 = true;
                    } else {
                        fail("center of path is neither v2 or v3 but found " + id);
                    }
                } else if (i == 2) {
                    assertEquals(id, v2.getId());
                } else if (i == 3) {
                    assertEquals(id, v5.getId());
                }
                i++;
            }
        }
        assertTrue("v3 not found in path", found3);
        assertTrue("v4 not found in path", found4);
    }

    @Test
    public void testGetVerticesFromVertex() {
        Vertex v1 = graph.addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A);
        Vertex v2 = graph.addVertex("v2", VISIBILITY_A, AUTHORIZATIONS_A);
        Vertex v3 = graph.addVertex("v3", VISIBILITY_A, AUTHORIZATIONS_A);
        Vertex v4 = graph.addVertex("v4", VISIBILITY_A, AUTHORIZATIONS_A);
        graph.addEdge(v1, v2, "knows", VISIBILITY_A, AUTHORIZATIONS_A);
        graph.addEdge(v1, v3, "knows", VISIBILITY_A, AUTHORIZATIONS_A);
        graph.addEdge(v1, v4, "knows", VISIBILITY_A, AUTHORIZATIONS_A);
        graph.addEdge(v2, v3, "knows", VISIBILITY_A, AUTHORIZATIONS_A);

        v1 = graph.getVertex("v1", AUTHORIZATIONS_A);
        assertEquals(3, count(v1.getVertices(Direction.BOTH, AUTHORIZATIONS_A)));
        assertEquals(3, count(v1.getVertices(Direction.OUT, AUTHORIZATIONS_A)));
        assertEquals(0, count(v1.getVertices(Direction.IN, AUTHORIZATIONS_A)));

        v2 = graph.getVertex("v2", AUTHORIZATIONS_A);
        assertEquals(2, count(v2.getVertices(Direction.BOTH, AUTHORIZATIONS_A)));
        assertEquals(1, count(v2.getVertices(Direction.OUT, AUTHORIZATIONS_A)));
        assertEquals(1, count(v2.getVertices(Direction.IN, AUTHORIZATIONS_A)));

        v3 = graph.getVertex("v3", AUTHORIZATIONS_A);
        assertEquals(2, count(v3.getVertices(Direction.BOTH, AUTHORIZATIONS_A)));
        assertEquals(0, count(v3.getVertices(Direction.OUT, AUTHORIZATIONS_A)));
        assertEquals(2, count(v3.getVertices(Direction.IN, AUTHORIZATIONS_A)));

        v4 = graph.getVertex("v4", AUTHORIZATIONS_A);
        assertEquals(1, count(v4.getVertices(Direction.BOTH, AUTHORIZATIONS_A)));
        assertEquals(0, count(v4.getVertices(Direction.OUT, AUTHORIZATIONS_A)));
        assertEquals(1, count(v4.getVertices(Direction.IN, AUTHORIZATIONS_A)));
    }

    @Test
    public void testBlankVisibilityString() {
        Vertex v = graph.addVertex("v1", VISIBILITY_EMPTY, AUTHORIZATIONS_EMPTY);
        assertNotNull(v);
        assertEquals("v1", v.getId());

        v = graph.getVertex("v1", AUTHORIZATIONS_EMPTY);
        assertNotNull(v);
        assertEquals("v1", v.getId());
        assertEquals(VISIBILITY_EMPTY, v.getVisibility());
    }

    @Test
    public void testElementMutationDoesntChangeObjectUntilSave() {
        Vertex v = graph.addVertex("v1", VISIBILITY_EMPTY, AUTHORIZATIONS_EMPTY);
        v.setProperty("prop1", "value1", VISIBILITY_A);

        ElementMutation<Vertex> m = v.prepareMutation()
                .setProperty("prop1", "value2", VISIBILITY_A)
                .setProperty("prop2", "value2", VISIBILITY_A);
        assertEquals(1, count(v.getProperties()));
        assertEquals("value1", v.getPropertyValue("prop1"));

        m.save();
        assertEquals(2, count(v.getProperties()));
        assertEquals("value2", v.getPropertyValue("prop1"));
        assertEquals("value2", v.getPropertyValue("prop2"));
    }

    @Test
    public void testFindRelatedEdges() {
        Vertex v1 = graph.addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A);
        Vertex v2 = graph.addVertex("v2", VISIBILITY_A, AUTHORIZATIONS_A);
        Vertex v3 = graph.addVertex("v3", VISIBILITY_A, AUTHORIZATIONS_A);
        Vertex v4 = graph.addVertex("v4", VISIBILITY_A, AUTHORIZATIONS_A);
        Edge ev1v2 = graph.addEdge("e v1->v2", v1, v2, "", VISIBILITY_A, AUTHORIZATIONS_A);
        Edge ev1v3 = graph.addEdge("e v1->v3", v1, v3, "", VISIBILITY_A, AUTHORIZATIONS_A);
        Edge ev2v3 = graph.addEdge("e v2->v3", v2, v3, "", VISIBILITY_A, AUTHORIZATIONS_A);
        Edge ev3v1 = graph.addEdge("e v3->v1", v3, v1, "", VISIBILITY_A, AUTHORIZATIONS_A);
        graph.addEdge("e v3->v4", v3, v4, "", VISIBILITY_A, AUTHORIZATIONS_A);

        List<Object> vertexIds = new ArrayList<Object>();
        vertexIds.add("v1");
        vertexIds.add("v2");
        vertexIds.add("v3");
        Iterable<Object> edges = toList(graph.findRelatedEdges(vertexIds, AUTHORIZATIONS_A));
        assertEquals(4, count(edges));
        assertContains(ev1v2.getId(), edges);
        assertContains(ev1v3.getId(), edges);
        assertContains(ev2v3.getId(), edges);
        assertContains(ev3v1.getId(), edges);
    }

    @Test
    public void testEmptyPropertyMutation() {
        Vertex v1 = graph.addVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A);
        v1.prepareMutation().save();
    }

    @Test
    public void testTextIndex() {
        graph.prepareVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A)
                .setProperty("none", new Text("Test Value", TextIndexHint.NONE), VISIBILITY_A)
                .setProperty("both", new Text("Test Value", TextIndexHint.ALL), VISIBILITY_A)
                .setProperty("fullText", new Text("Test Value", TextIndexHint.FULL_TEXT), VISIBILITY_A)
                .setProperty("exactMatch", new Text("Test Value", TextIndexHint.EXACT_MATCH), VISIBILITY_A)
                .save();

        Vertex v1 = graph.getVertex("v1", AUTHORIZATIONS_A);
        assertEquals("Test Value", v1.getPropertyValue("none"));
        assertEquals("Test Value", v1.getPropertyValue("both"));
        assertEquals("Test Value", v1.getPropertyValue("fullText"));
        assertEquals("Test Value", v1.getPropertyValue("exactMatch"));

        assertEquals(1, count(graph.query(AUTHORIZATIONS_A).has("both", TextPredicate.CONTAINS, "Test").vertices()));
        assertEquals(1, count(graph.query(AUTHORIZATIONS_A).has("fullText", TextPredicate.CONTAINS, "Test").vertices()));
        assertEquals("exact match shouldn't match partials", 0, count(graph.query(AUTHORIZATIONS_A).has("exactMatch", TextPredicate.CONTAINS, "Test").vertices()));
        assertEquals("unindexed property shouldn't match partials", 0, count(graph.query(AUTHORIZATIONS_A).has("none", TextPredicate.CONTAINS, "Test").vertices()));

        assertEquals(1, count(graph.query(AUTHORIZATIONS_A).has("both", "Test Value").vertices()));
        assertEquals("default has predicate is equals which shouldn't work for full text", 0, count(graph.query(AUTHORIZATIONS_A).has("fullText", "Test Value").vertices()));
        assertEquals(1, count(graph.query(AUTHORIZATIONS_A).has("exactMatch", "Test Value").vertices()));
        assertEquals("default has predicate is equals which shouldn't work for unindexed", 0, count(graph.query(AUTHORIZATIONS_A).has("none", "Test Value").vertices()));
    }

    @Test
    public void testChangeVisibilityVertex() {
        graph.prepareVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A)
                .save();

        Vertex v1 = graph.getVertex("v1", AUTHORIZATIONS_A);
        v1.prepareMutation()
                .alterElementVisibility(VISIBILITY_B)
                .save();

        v1 = graph.getVertex("v1", AUTHORIZATIONS_A);
        assertNull(v1);
        v1 = graph.getVertex("v1", AUTHORIZATIONS_B);
        assertNotNull(v1);

        // change to same visibility
        v1 = graph.getVertex("v1", AUTHORIZATIONS_B);
        v1.prepareMutation()
                .alterElementVisibility(VISIBILITY_B)
                .save();

        v1 = graph.getVertex("v1", AUTHORIZATIONS_A);
        assertNull(v1);
        v1 = graph.getVertex("v1", AUTHORIZATIONS_B);
        assertNotNull(v1);
    }

    @Test
    public void testChangeVisibilityVertexProperties() {
        Map<String, Object> prop1Metadata = new HashMap<String, Object>();
        prop1Metadata.put("prop1_key1", "value1");

        Map<String, Object> prop2Metadata = new HashMap<String, Object>();
        prop2Metadata.put("prop2_key1", "value1");

        graph.prepareVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A)
                .setProperty("prop1", "value1", prop1Metadata, VISIBILITY_EMPTY)
                .setProperty("prop2", "value2", prop2Metadata, VISIBILITY_EMPTY)
                .save();

        Vertex v1 = graph.getVertex("v1", AUTHORIZATIONS_A);
        v1.prepareMutation()
                .alterPropertyVisibility("prop1", VISIBILITY_B)
                .save();

        v1 = graph.getVertex("v1", AUTHORIZATIONS_A);
        assertNull(v1.getProperty("prop1"));
        assertNotNull(v1.getProperty("prop2"));

        v1 = graph.getVertex("v1", AUTHORIZATIONS_A_AND_B);
        assertNotNull(v1.getProperty("prop1"));
        assertNotNull(v1.getProperty("prop2"));

        // alter and set property in one mutation
        v1 = graph.getVertex("v1", AUTHORIZATIONS_A_AND_B);
        v1.prepareMutation()
                .alterPropertyVisibility("prop1", VISIBILITY_A)
                .setProperty("prop1", "value1New", VISIBILITY_A)
                .save();

        v1 = graph.getVertex("v1", AUTHORIZATIONS_A_AND_B);
        assertNotNull(v1.getProperty("prop1"));
        assertEquals("value1New", v1.getPropertyValue("prop1"));

        // alter visibility to the same visibility
        v1 = graph.getVertex("v1", AUTHORIZATIONS_A_AND_B);
        v1.prepareMutation()
                .alterPropertyVisibility("prop1", VISIBILITY_A)
                .setProperty("prop1", "value1New2", VISIBILITY_A)
                .save();

        v1 = graph.getVertex("v1", AUTHORIZATIONS_A_AND_B);
        assertNotNull(v1.getProperty("prop1"));
        assertEquals("value1New2", v1.getPropertyValue("prop1"));
    }

    @Test
    public void testChangeVisibilityEdge() {
        Vertex v1 = graph.prepareVertex("v1", VISIBILITY_EMPTY, AUTHORIZATIONS_A)
                .save();

        Vertex v2 = graph.prepareVertex("v2", VISIBILITY_EMPTY, AUTHORIZATIONS_A)
                .save();

        graph.prepareEdge("e1", v1, v2, "", VISIBILITY_A, AUTHORIZATIONS_A)
                .save();

        // test that we can see the edge with A and not B
        v1 = graph.getVertex("v1", AUTHORIZATIONS_A);
        assertEquals(0, count(v1.getEdges(Direction.BOTH, AUTHORIZATIONS_B)));
        v1 = graph.getVertex("v1", AUTHORIZATIONS_A);
        assertEquals(1, count(v1.getEdges(Direction.BOTH, AUTHORIZATIONS_A)));

        // change the edge
        Edge e1 = graph.getEdge("e1", AUTHORIZATIONS_A);
        e1.prepareMutation()
                .alterElementVisibility(VISIBILITY_B)
                .save();

        // test that we can see the edge with B and not A
        v1 = graph.getVertex("v1", AUTHORIZATIONS_B);
        assertEquals(1, count(v1.getEdges(Direction.BOTH, AUTHORIZATIONS_B)));
        v1 = graph.getVertex("v1", AUTHORIZATIONS_A);
        assertEquals(0, count(v1.getEdges(Direction.BOTH, AUTHORIZATIONS_A)));

        // change the edge visibility to same
        e1 = graph.getEdge("e1", AUTHORIZATIONS_B);
        e1.prepareMutation()
                .alterElementVisibility(VISIBILITY_B)
                .save();

        // test that we can see the edge with B and not A
        v1 = graph.getVertex("v1", AUTHORIZATIONS_B);
        assertEquals(1, count(v1.getEdges(Direction.BOTH, AUTHORIZATIONS_B)));
        v1 = graph.getVertex("v1", AUTHORIZATIONS_A);
        assertEquals(0, count(v1.getEdges(Direction.BOTH, AUTHORIZATIONS_A)));
    }

    @Test
    public void testChangeVisibilityOnBadPropertyName() {
        graph.prepareVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A)
                .setProperty("prop1", "value1", VISIBILITY_EMPTY)
                .setProperty("prop2", "value2", VISIBILITY_B)
                .save();

        try {
            graph.getVertex("v1", AUTHORIZATIONS_A)
                    .prepareMutation()
                    .alterPropertyVisibility("propBad", VISIBILITY_B)
                    .save();
            fail("show throw");
        } catch (SecureGraphException ex) {
            assertNotNull(ex);
        }
    }

    @Test
    public void testChangeVisibilityOnStreamingProperty() throws IOException {
        String expectedLargeValue = IOUtils.toString(new LargeStringInputStream(LARGE_PROPERTY_VALUE_SIZE));
        PropertyValue propSmall = new StreamingPropertyValue(new ByteArrayInputStream("value1".getBytes()), String.class);
        PropertyValue propLarge = new StreamingPropertyValue(new ByteArrayInputStream(expectedLargeValue.getBytes()), String.class);
        String largePropertyName = "propLarge/\\*!@#$%^&*()[]{}|";
        graph.prepareVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A)
                .setProperty("propSmall", propSmall, VISIBILITY_A)
                .setProperty(largePropertyName, propLarge, VISIBILITY_A)
                .save();
        assertEquals(2, count(graph.getVertex("v1", AUTHORIZATIONS_A).getProperties()));

        graph.getVertex("v1", AUTHORIZATIONS_A)
                .prepareMutation()
                .alterPropertyVisibility("propSmall", VISIBILITY_B)
                .save();
        assertEquals(1, count(graph.getVertex("v1", AUTHORIZATIONS_A).getProperties()));

        graph.getVertex("v1", AUTHORIZATIONS_A)
                .prepareMutation()
                .alterPropertyVisibility(largePropertyName, VISIBILITY_B)
                .save();
        assertEquals(0, count(graph.getVertex("v1", AUTHORIZATIONS_A).getProperties()));

        assertEquals(2, count(graph.getVertex("v1", AUTHORIZATIONS_A_AND_B).getProperties()));
    }

    @Test
    public void testChangePropertyMetadata() {
        Map<String, Object> prop1Metadata = new HashMap<String, Object>();
        prop1Metadata.put("prop1_key1", "valueOld");

        graph.prepareVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A)
                .setProperty("prop1", "value1", prop1Metadata, VISIBILITY_EMPTY)
                .setProperty("prop2", "value2", null, VISIBILITY_EMPTY)
                .save();

        Vertex v1 = graph.getVertex("v1", AUTHORIZATIONS_A);
        v1.prepareMutation()
                .alterPropertyMetadata("prop1", "prop1_key1", "valueNew")
                .save();
        assertEquals("valueNew", v1.getProperty("prop1").getMetadata().get("prop1_key1"));

        v1 = graph.getVertex("v1", AUTHORIZATIONS_A);
        assertEquals("valueNew", v1.getProperty("prop1").getMetadata().get("prop1_key1"));

        v1 = graph.getVertex("v1", AUTHORIZATIONS_A);
        v1.prepareMutation()
                .alterPropertyMetadata("prop2", "prop2_key1", "valueNew")
                .save();
        assertEquals("valueNew", v1.getProperty("prop2").getMetadata().get("prop2_key1"));

        v1 = graph.getVertex("v1", AUTHORIZATIONS_A);
        assertEquals("valueNew", v1.getProperty("prop2").getMetadata().get("prop2_key1"));
    }

    @Test
    public void testIsVisibilityValid() {
        assertFalse(graph.isVisibilityValid(VISIBILITY_A, AUTHORIZATIONS_C));
        assertTrue(graph.isVisibilityValid(VISIBILITY_B, AUTHORIZATIONS_A_AND_B));
        assertTrue(graph.isVisibilityValid(VISIBILITY_B, AUTHORIZATIONS_B));
        assertTrue(graph.isVisibilityValid(VISIBILITY_EMPTY, AUTHORIZATIONS_A));
    }
}
