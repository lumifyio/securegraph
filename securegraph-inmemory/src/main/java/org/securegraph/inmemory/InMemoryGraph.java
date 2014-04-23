package org.securegraph.inmemory;

import org.securegraph.*;
import org.securegraph.id.IdGenerator;
import org.securegraph.id.UUIDIdGenerator;
import org.securegraph.mutation.AlterPropertyMetadata;
import org.securegraph.mutation.AlterPropertyVisibility;
import org.securegraph.search.DefaultSearchIndex;
import org.securegraph.search.SearchIndex;
import org.securegraph.util.LookAheadIterable;

import java.util.*;

import static org.securegraph.util.IterableUtils.toList;
import static org.securegraph.util.Preconditions.checkNotNull;

public class InMemoryGraph extends GraphBase {
    private static final InMemoryGraphConfiguration DEFAULT_CONFIGURATION = new InMemoryGraphConfiguration(new HashMap());
    private final Map<Object, InMemoryVertex> vertices;
    private final Map<Object, InMemoryEdge> edges;

    public InMemoryGraph() {
        this(DEFAULT_CONFIGURATION, new UUIDIdGenerator(DEFAULT_CONFIGURATION.getConfig()), new DefaultSearchIndex(DEFAULT_CONFIGURATION.getConfig()));
    }

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
                Iterable<Property> properties = getProperties();
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
                return canRead(src.getVisibility(), authorizations);
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
        if (!canRead(vertex.getVisibility(), authorizations)) {
            return;
        }

        List<Edge> edgesToRemove = toList(vertex.getEdges(Direction.BOTH, authorizations));
        for (Edge edgeToRemove : edgesToRemove) {
            removeEdge(edgeToRemove, authorizations);
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
                return canRead(src.getVisibility(), authorizations);
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
        if (!canRead(edge.getVisibility(), authorizations)) {
            return;
        }

        Vertex inVertex = getVertex(edge.getVertexId(Direction.IN), authorizations);
        checkNotNull(inVertex, "Could not find in vertex: " + edge.getVertexId(Direction.IN));
        Vertex outVertex = getVertex(edge.getVertexId(Direction.OUT), authorizations);
        checkNotNull(outVertex, "Could not find out vertex: " + edge.getVertexId(Direction.OUT));

        this.edges.remove(edge.getId());
        getSearchIndex().removeElement(this, edge);
    }

    @Override
    public void flush() {
        if (getSearchIndex() != null) {
            getSearchIndex().flush();
        }
    }

    @Override
    public void shutdown() {
        flush();
        if (getSearchIndex() != null) {
            getSearchIndex().shutdown();
        }
    }

    public Iterable<Edge> getEdgesFromVertex(final Object vertexId, final Authorizations authorizations) {
        return new LookAheadIterable<InMemoryEdge, Edge>() {
            @Override
            protected boolean isIncluded(InMemoryEdge src, Edge edge) {
                Object inVertexId = src.getVertexId(Direction.IN);
                checkNotNull(inVertexId, "inVertexId was null");
                Object outVertexId = src.getVertexId(Direction.OUT);
                checkNotNull(outVertexId, "outVertexId was null");

                if (!inVertexId.equals(vertexId) && !outVertexId.equals(vertexId)) {
                    return false;
                }
                return canRead(src.getVisibility(), authorizations);
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

    private boolean canRead(Visibility visibility, Authorizations authorizations) {
        // this is just a shortcut so that we don't need to construct evaluators and visibility objects to check for an empty string.
        if (visibility.getVisibilityString().length() == 0) {
            return true;
        }

        return authorizations.canRead(visibility);
    }

    public void saveProperties(Element element, Iterable<Property> properties) {
        if (element instanceof Vertex) {
            InMemoryVertex vertex = vertices.get(element.getId());
            vertex.updatePropertiesInternal(properties);
        } else if (element instanceof Edge) {
            InMemoryEdge edge = edges.get(element.getId());
            edge.updatePropertiesInternal(properties);
        } else {
            throw new IllegalArgumentException("Unexpected element type: " + element.getClass().getName());
        }
        getSearchIndex().addElement(this, element);
    }

    public void removeProperty(Element element, Property property) {
        if (element instanceof Vertex) {
            InMemoryVertex vertex = vertices.get(element.getId());
            vertex.removePropertyInternal(property.getKey(), property.getName());
        } else if (element instanceof Edge) {
            InMemoryEdge edge = edges.get(element.getId());
            edge.removePropertyInternal(property.getKey(), property.getName());
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
            if (canRead(p.getVisibility(), authorizations)) {
                filteredProperties.add(p);
            }
        }
        return filteredProperties;
    }

    public Map<Object, InMemoryVertex> getAllVertices() {
        return this.vertices;
    }

    public Map<Object, InMemoryEdge> getAllEdges() {
        return this.edges;
    }

    void alterEdgeVisibility(Object edgeId, Visibility newEdgeVisibility) {
        this.edges.get(edgeId).setVisibilityInternal(newEdgeVisibility);
    }

    void alterVertexVisibility(Object vertexId, Visibility newVertexVisibility) {
        this.vertices.get(vertexId).setVisibilityInternal(newVertexVisibility);
    }

    void alterEdgePropertyVisibilities(Object edgeId, List<AlterPropertyVisibility> alterPropertyVisibilities) {
        alterElementPropertyVisibilities(this.edges.get(edgeId), alterPropertyVisibilities);
    }

    void alterVertexPropertyVisibilities(Object vertexId, List<AlterPropertyVisibility> alterPropertyVisibilities) {
        alterElementPropertyVisibilities(this.vertices.get(vertexId), alterPropertyVisibilities);
    }

    void alterElementPropertyVisibilities(InMemoryElement element, List<AlterPropertyVisibility> alterPropertyVisibilities) {
        for (AlterPropertyVisibility apv : alterPropertyVisibilities) {
            Property property = element.getProperty(apv.getKey(), apv.getName(), apv.getExistingVisibility());
            if (property == null) {
                throw new SecureGraphException("Could not find property " + apv.getKey() + ":" + apv.getName());
            }
            Object value = property.getValue();
            Map<String, Object> metadata = property.getMetadata();

            element.removeProperty(apv.getKey(), apv.getName());
            element.addPropertyValue(apv.getKey(), apv.getName(), value, metadata, apv.getVisibility());
        }
    }

    public void alterEdgePropertyMetadata(Object edgeId, List<AlterPropertyMetadata> alterPropertyMetadatas) {
        alterElementPropertyMetadata(this.edges.get(edgeId), alterPropertyMetadatas);
    }

    public void alterVertexPropertyMetadata(Object vertexId, List<AlterPropertyMetadata> alterPropertyMetadatas) {
        alterElementPropertyMetadata(this.vertices.get(vertexId), alterPropertyMetadatas);
    }

    private void alterElementPropertyMetadata(Element element, List<AlterPropertyMetadata> alterPropertyMetadatas) {
        for (AlterPropertyMetadata apm : alterPropertyMetadatas) {
            Property property = element.getProperty(apm.getPropertyKey(), apm.getPropertyName(), apm.getPropertyVisibility());
            if (property == null) {
                throw new SecureGraphException("Could not find property " + apm.getPropertyKey() + ":" + apm.getPropertyName());
            }

            property.getMetadata().put(apm.getMetadataName(), apm.getNewValue());
        }
    }

    @Override
    public boolean isVisibilityValid(Visibility visibility, Authorizations authorizations) {
        return authorizations.canRead(visibility);
    }
}
