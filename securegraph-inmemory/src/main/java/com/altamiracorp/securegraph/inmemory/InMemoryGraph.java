package com.altamiracorp.securegraph.inmemory;

import com.altamiracorp.securegraph.*;
import com.altamiracorp.securegraph.id.IdGenerator;
import com.altamiracorp.securegraph.search.SearchIndex;
import com.altamiracorp.securegraph.util.LookAheadIterable;

import java.util.*;

public class InMemoryGraph extends GraphBase {
    private final Map<Object, InMemoryVertex> vertices;
    private final Map<Object, InMemoryEdge> edges;

    public InMemoryGraph(InMemoryGraphConfiguration configuration, IdGenerator idGenerator, SearchIndex searchIndex) {
        this(configuration, idGenerator, searchIndex, new HashMap<Object, InMemoryVertex>(), new HashMap<Object, InMemoryEdge>());
    }

    protected InMemoryGraph(InMemoryGraphConfiguration configuration, IdGenerator idGenerator, SearchIndex searchIndex, Map<Object, InMemoryVertex> vertices, Map<Object, InMemoryEdge> edges) {
        super(configuration, idGenerator, searchIndex);
        this.vertices = vertices;
        this.edges = edges;
    }

    public static InMemoryGraph create(InMemoryGraphConfiguration config) {
        IdGenerator idGenerator = config.createIdGenerator();
        SearchIndex searchIndex = config.createSearchIndex();
        return new InMemoryGraph(config, idGenerator, searchIndex);
    }

    public static InMemoryGraph create(Map config) {
        return create(new InMemoryGraphConfiguration(config));
    }

    @Override
    public VertexBuilder prepareVertex(Object vertexId, Visibility visibility, Authorizations authorizations) {
        if (vertexId == null) {
            vertexId = getIdGenerator().nextId();
        }

        return new VertexBuilder(vertexId, visibility) {
            @Override
            public Vertex save() {
                List<Property> properties = getProperties();
                InMemoryVertex vertex = new InMemoryVertex(InMemoryGraph.this, getVertexId(), getVisibility(), properties);
                vertices.put(getVertexId(), vertex);

                getSearchIndex().addElement(InMemoryGraph.this, vertex);

                return vertex;
            }
        };
    }

    @Override
    public Iterable<Vertex> getVertices(final Authorizations authorizations) throws SecureGraphException {
        return new LookAheadIterable<InMemoryVertex, Vertex>() {
            @Override
            protected boolean isIncluded(InMemoryVertex src, Vertex edge) {
                return hasAccess(src.getVisibility(), authorizations);
            }

            @Override
            protected Vertex convert(InMemoryVertex vertex) {
                return filteredVertex(vertex, authorizations);
            }

            @Override
            protected Iterator<InMemoryVertex> createIterator() {
                return vertices.values().iterator();
            }
        };
    }

    @Override
    public void removeVertex(Vertex vertex, Authorizations authorizations) {
        if (!hasAccess(vertex.getVisibility(), authorizations)) {
            return;
        }
        this.vertices.remove(vertex.getId());
        getSearchIndex().removeElement(this, vertex);
    }

    @Override
    public EdgeBuilder prepareEdge(Object edgeId, Vertex outVertex, Vertex inVertex, String label, Visibility visibility, Authorizations authorizations) {
        if (edgeId == null) {
            edgeId = getIdGenerator().nextId();
        }

        return new EdgeBuilder(edgeId, outVertex, inVertex, label, visibility) {
            @Override
            public Edge save() {
                InMemoryEdge edge = new InMemoryEdge(InMemoryGraph.this, getEdgeId(), getOutVertex().getId(), getInVertex().getId(), getLabel(), getVisibility(), getProperties());
                edges.put(getEdgeId(), edge);

                getSearchIndex().addElement(InMemoryGraph.this, edge);

                return edge;
            }
        };
    }

    @Override
    public Iterable<Edge> getEdges(final Authorizations authorizations) {
        return new LookAheadIterable<InMemoryEdge, Edge>() {
            @Override
            protected boolean isIncluded(InMemoryEdge src, Edge edge) {
                return hasAccess(src.getVisibility(), authorizations);
            }

            @Override
            protected Edge convert(InMemoryEdge edge) {
                return filteredEdge(edge, authorizations);
            }

            @Override
            protected Iterator<InMemoryEdge> createIterator() {
                return edges.values().iterator();
            }
        };
    }

    @Override
    public void removeEdge(Edge edge, Authorizations authorizations) {
        if (!hasAccess(edge.getVisibility(), authorizations)) {
            return;
        }
        this.edges.remove(edge.getId());
        getSearchIndex().removeElement(this, edge);
    }

    @Override
    public void flush() {
        getSearchIndex().flush();
    }

    @Override
    public void shutdown() {

    }

    public Iterable<Edge> getEdgesFromVertex(final Object vertexId, final Authorizations authorizations) {
        return new LookAheadIterable<InMemoryEdge, Edge>() {
            @Override
            protected boolean isIncluded(InMemoryEdge src, Edge edge) {
                if (!src.getVertexId(Direction.IN).equals(vertexId) && !src.getVertexId(Direction.OUT).equals(vertexId)) {
                    return false;
                }
                return hasAccess(src.getVisibility(), authorizations);
            }

            @Override
            protected Edge convert(InMemoryEdge edge) {
                return filteredEdge(edge, authorizations);
            }

            @Override
            protected Iterator<InMemoryEdge> createIterator() {
                return edges.values().iterator();
            }
        };
    }

    private boolean hasAccess(Visibility visibility, Authorizations authorizations) {
        // TODO handle more complex accessibility. borrow code from Accumulo?
        for (String a : authorizations.getAuthorizations()) {
            if (visibility.getVisibilityString().equals(a)) {
                return true;
            }
        }
        return false;
    }

    public void saveProperties(Element element, List<Property> properties) {
        if (element instanceof Vertex) {
            InMemoryVertex vertex = vertices.get(element.getId());
            vertex.setPropertiesInternal(properties);
        } else if (element instanceof Edge) {
            InMemoryEdge edge = edges.get(element.getId());
            edge.setPropertiesInternal(properties);
        } else {
            throw new IllegalArgumentException("Unexpected element type: " + element.getClass().getName());
        }
        getSearchIndex().addElement(this, element);
    }

    public void removeProperty(Element element, Property property) {
        if (element instanceof Vertex) {
            InMemoryVertex vertex = vertices.get(element.getId());
            vertex.removePropertyInternal(property.getId(), property.getName());
        } else if (element instanceof Edge) {
            InMemoryEdge edge = edges.get(element.getId());
            edge.removePropertyInternal(property.getId(), property.getName());
        } else {
            throw new IllegalArgumentException("Unexpected element type: " + element.getClass().getName());
        }
        getSearchIndex().removeElement(this, element);
    }

    private Edge filteredEdge(InMemoryEdge edge, Authorizations authorizations) {
        Object edgeId = edge.getId();
        Object outVertexId = edge.getVertexId(Direction.OUT);
        Object inVertexId = edge.getVertexId(Direction.IN);
        String label = edge.getLabel();
        Visibility visibility = edge.getVisibility();
        List<Property> properties = filterProperties(edge.getProperties(), authorizations);
        return new InMemoryEdge(this, edgeId, outVertexId, inVertexId, label, visibility, properties);
    }

    private Vertex filteredVertex(InMemoryVertex vertex, Authorizations authorizations) {
        Object vertexId = vertex.getId();
        Visibility visibility = vertex.getVisibility();
        List<Property> properties = filterProperties(vertex.getProperties(), authorizations);
        return new InMemoryVertex(this, vertexId, visibility, properties);
    }

    private List<Property> filterProperties(Iterable<Property> properties, Authorizations authorizations) {
        List<Property> filteredProperties = new ArrayList<Property>();
        for (Property p : properties) {
            if (hasAccess(p.getVisibility(), authorizations)) {
                filteredProperties.add(p);
            }
        }
        return filteredProperties;
    }
}
