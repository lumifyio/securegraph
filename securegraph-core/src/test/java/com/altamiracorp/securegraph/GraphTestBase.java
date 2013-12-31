package com.altamiracorp.securegraph;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static com.altamiracorp.securegraph.util.IterableUtils.assertContains;
import static com.altamiracorp.securegraph.util.IterableUtils.count;
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

    protected abstract Graph createGraph();

    @Before
    public void before() {
        graph = createGraph();
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

        v.addProperties(
                new Property("prop1", "value1a", VISIBILITY_A),
                new Property("prop2", "value2a", VISIBILITY_A),
                new Property("prop3", "value3a", VISIBILITY_A));
        v = graph.getVertex("v1", AUTHORIZATIONS_A);
        assertEquals("value1a", v.getPropertyValues("prop1").iterator().next());
        assertEquals("value2a", v.getPropertyValues("prop2").iterator().next());
        assertEquals("value3a", v.getPropertyValues("prop3").iterator().next());
        assertEquals(3, count(v.getProperties()));

        v.setProperties(
                new Property("prop1", "value1b", VISIBILITY_A),
                new Property("prop2", "value2b", VISIBILITY_A));
        v = graph.getVertex("v1", AUTHORIZATIONS_A);
        assertEquals("value1b", v.getPropertyValues("prop1").iterator().next());
        assertEquals("value2b", v.getPropertyValues("prop2").iterator().next());
        assertEquals("value3a", v.getPropertyValues("prop3").iterator().next());
        assertEquals(3, count(v.getProperties()));

        v.addProperties(new Property("prop1", "value1a-new", VISIBILITY_A));
        v = graph.getVertex("v1", AUTHORIZATIONS_A);
        assertContains("value1a", v.getPropertyValues("prop1"));
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

        graph.removeVertex("v1", AUTHORIZATIONS_B);
        assertEquals(1, count(graph.getVertices(AUTHORIZATIONS_A)));

        graph.removeVertex("v1", AUTHORIZATIONS_A);
        assertEquals(0, count(graph.getVertices(AUTHORIZATIONS_A)));
    }

    @Test
    public void testRemoveVertexWithProperties() {
        graph.addVertex("v1", VISIBILITY_A,
                new Property("prop1", "value1", VISIBILITY_B));

        assertEquals(1, count(graph.getVertices(AUTHORIZATIONS_A)));

        graph.removeVertex("v1", AUTHORIZATIONS_B);
        assertEquals(1, count(graph.getVertices(AUTHORIZATIONS_A)));

        graph.removeVertex("v1", AUTHORIZATIONS_A);
        assertEquals(0, count(graph.getVertices(AUTHORIZATIONS_A)));
    }

    @Test
    public void testAddEdge() {
        Vertex v1 = graph.addVertex("v1", VISIBILITY_A);
        Vertex v2 = graph.addVertex("v2", VISIBILITY_A);
        Edge e = graph.addEdge("e1", v1, v2, "label1", VISIBILITY_A);
        assertNotNull(e);
        assertEquals("e1", e.getId());
        assertEquals("label1", e.getLabel());
        assertEquals("v1", e.getOutVertexId());
        assertEquals(v1, e.getOutVertex());
        assertEquals("v2", e.getInVertexId());
        assertEquals(v2, e.getInVertex());
        assertEquals(VISIBILITY_A, e.getVisibility());

        e = graph.getEdge("e1", AUTHORIZATIONS_B);
        assertNull(e);

        e = graph.getEdge("e1", AUTHORIZATIONS_A);
        assertNotNull(e);
        assertEquals("e1", e.getId());
        assertEquals("label1", e.getLabel());
        assertEquals("v1", e.getOutVertexId());
        assertEquals(v1, e.getOutVertex());
        assertEquals("v2", e.getInVertexId());
        assertEquals(v2, e.getInVertex());
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
        assertEquals("valueA", e.getPropertyValues("propA").iterator().next());
        assertEquals(1, count(e.getPropertyValues("propB")));

        e = graph.getEdge("e1", AUTHORIZATIONS_A_AND_B);
        assertEquals("valueA", e.getPropertyValues("propA").iterator().next());
        assertEquals("valueB", e.getPropertyValues("propB").iterator().next());
    }

    @Test
    public void testRemoveEdge() {
        Vertex v1 = graph.addVertex("v1", VISIBILITY_A);
        Vertex v2 = graph.addVertex("v2", VISIBILITY_A);
        graph.addEdge("e1", v1, v2, "label1", VISIBILITY_A);

        assertEquals(1, count(graph.getEdges(AUTHORIZATIONS_A)));

        graph.removeEdge("v1", AUTHORIZATIONS_B);
        assertEquals(1, count(graph.getEdges(AUTHORIZATIONS_A)));

        graph.removeEdge("v1", AUTHORIZATIONS_A);
        assertEquals(0, count(graph.getEdges(AUTHORIZATIONS_A)));
    }

    @Test
    public void testAddEdgeWithVisibility() {
        Vertex v1 = graph.addVertex("v1", VISIBILITY_A);
        Vertex v2 = graph.addVertex("v2", VISIBILITY_A);
        graph.addEdge(v1, v2, "edgeA", VISIBILITY_A);
        graph.addEdge(v1, v2, "edgeB", VISIBILITY_B);

        Iterable<Edge> aEdges = graph.getVertex("v1", AUTHORIZATIONS_A).getEdges(Direction.BOTH);
        assertEquals(1, count(aEdges));
        assertEquals("edgeA", aEdges.iterator().next().getLabel());

        Iterable<Edge> bEdges = graph.getVertex("v1", AUTHORIZATIONS_B).getEdges(Direction.BOTH);
        assertEquals(1, count(bEdges));
        assertEquals("edgeB", bEdges.iterator().next().getLabel());

        Iterable<Edge> allEdges = graph.getVertex("v1", AUTHORIZATIONS_A_AND_B).getEdges(Direction.BOTH);
        assertEquals(2, count(allEdges));
    }

    @Test
    public void testGraphQuery() {
        Vertex v1 = graph.addVertex("v1", VISIBILITY_A);
        Vertex v2 = graph.addVertex("v2", VISIBILITY_A);
        graph.addEdge(v1, v2, "edgeA", VISIBILITY_A);

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
        graph.addEdge(v1, v2, "edgeA", VISIBILITY_A);

        v1 = graph.getVertex("v1", AUTHORIZATIONS_A);
        Iterable<Vertex> vertices = v1.query().vertices();
        assertEquals(1, count(vertices));
        assertEquals("v2", vertices.iterator().next().getId());

        Iterable<Edge> edges = v1.query().edges();
        assertEquals(1, count(edges));

        edges = v1.query().edges(Direction.OUT);
        assertEquals(1, count(edges));
    }
}
