package org.securegraph.inmemory;

import org.securegraph.*;
import org.securegraph.event.*;
import org.securegraph.id.IdGenerator;
import org.securegraph.id.UUIDIdGenerator;
import org.securegraph.mutation.AlterPropertyMetadata;
import org.securegraph.mutation.AlterPropertyVisibility;
import org.securegraph.search.DefaultSearchIndex;
import org.securegraph.search.IndexHint;
import org.securegraph.search.SearchIndex;
import org.securegraph.util.LookAheadIterable;

import java.util.*;

import static org.securegraph.util.IterableUtils.toList;
import static org.securegraph.util.Preconditions.checkNotNull;

public class InMemoryGraph extends GraphBaseWithSearchIndex {
    private static final InMemoryGraphConfiguration DEFAULT_CONFIGURATION = new InMemoryGraphConfiguration(new HashMap());
    private final Map<String, InMemoryVertex> vertices;
    private final Map<String, InMemoryEdge> edges;

    public InMemoryGraph() {
        this(DEFAULT_CONFIGURATION, new UUIDIdGenerator(DEFAULT_CONFIGURATION.getConfig()), new DefaultSearchIndex(DEFAULT_CONFIGURATION.getConfig()));
    }

    public InMemoryGraph(InMemoryGraphConfiguration configuration, IdGenerator idGenerator, SearchIndex searchIndex) {
        this(configuration, idGenerator, searchIndex, new HashMap<String, InMemoryVertex>(), new HashMap<String, InMemoryEdge>());
    }

    protected InMemoryGraph(InMemoryGraphConfiguration configuration, IdGenerator idGenerator, SearchIndex searchIndex, Map<String, InMemoryVertex> vertices, Map<String, InMemoryEdge> edges) {
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
    public VertexBuilder prepareVertex(String vertexId, Visibility visibility) {
        if (vertexId == null) {
            vertexId = getIdGenerator().nextId();
        }

        return new VertexBuilder(vertexId, visibility) {
            @Override
            public Vertex save(Authorizations authorizations) {
                Vertex existingVertex = getVertex(getVertexId(), authorizations);

                Iterable<Property> properties;
                if (existingVertex == null) {
                    properties = getProperties();
                } else {
                    Iterable<Property> existingProperties = existingVertex.getProperties();
                    Iterable<Property> newProperties = getProperties();
                    properties = new TreeSet<Property>(toList(existingProperties));
                    for (Property p : newProperties) {
                        ((TreeSet<Property>) properties).remove(p);
                        ((TreeSet<Property>) properties).add(p);
                    }
                }

                InMemoryVertex vertex = new InMemoryVertex(InMemoryGraph.this, getVertexId(), getVisibility(), properties, authorizations);
                vertices.put(getVertexId(), vertex);

                if (getIndexHint() != IndexHint.DO_NOT_INDEX) {
                    getSearchIndex().addElement(InMemoryGraph.this, vertex, authorizations);
                }

                if (hasEventListeners()) {
                    fireGraphEvent(new AddVertexEvent(InMemoryGraph.this, vertex));
                    for (Property property : getProperties()) {
                        fireGraphEvent(new AddPropertyEvent(InMemoryGraph.this, vertex, property));
                    }
                }

                return vertex;
            }
        };
    }

    @Override
    public Iterable<Vertex> getVertices(EnumSet<FetchHint> fetchHints, final Authorizations authorizations) throws SecureGraphException {
        return new LookAheadIterable<InMemoryVertex, Vertex>() {
            @Override
            protected boolean isIncluded(InMemoryVertex src, Vertex vertex) {
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
        getSearchIndex().removeElement(this, vertex, authorizations);

        if (hasEventListeners()) {
            fireGraphEvent(new RemoveVertexEvent(this, vertex));
        }
    }

    @Override
    public EdgeBuilder prepareEdge(String edgeId, Vertex outVertex, Vertex inVertex, String label, Visibility visibility) {
        if (edgeId == null) {
            edgeId = getIdGenerator().nextId();
        }

        return new EdgeBuilder(edgeId, outVertex, inVertex, label, visibility) {
            @Override
            public Edge save(Authorizations authorizations) {
                Edge existingEdge = getEdge(getEdgeId(), authorizations);

                Iterable<Property> properties;
                if (existingEdge == null) {
                    properties = getProperties();
                } else {
                    Iterable<Property> existingProperties = existingEdge.getProperties();
                    Iterable<Property> newProperties = getProperties();
                    properties = new TreeSet<Property>(toList(existingProperties));
                    for (Property p : newProperties) {
                        ((TreeSet<Property>) properties).remove(p);
                        ((TreeSet<Property>) properties).add(p);
                    }
                }

                InMemoryEdge edge = new InMemoryEdge(InMemoryGraph.this, getEdgeId(), getOutVertex().getId(), getInVertex().getId(), getLabel(), getVisibility(), properties, authorizations);
                edges.put(getEdgeId(), edge);

                if (getIndexHint() != IndexHint.DO_NOT_INDEX) {
                    getSearchIndex().addElement(InMemoryGraph.this, edge, authorizations);
                }

                if (hasEventListeners()) {
                    fireGraphEvent(new AddEdgeEvent(InMemoryGraph.this, edge));
                    for (Property property : getProperties()) {
                        fireGraphEvent(new AddPropertyEvent(InMemoryGraph.this, edge, property));
                    }
                }

                return edge;
            }
        };
    }

    @Override
    public Iterable<Edge> getEdges(EnumSet<FetchHint> fetchHints, final Authorizations authorizations) {
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
        getSearchIndex().removeElement(this, edge, authorizations);

        if (hasEventListeners()) {
            fireGraphEvent(new RemoveEdgeEvent(this, edge));
        }
    }

    public Iterable<Edge> getEdgesFromVertex(final String vertexId, final Authorizations authorizations) {
        return new LookAheadIterable<InMemoryEdge, Edge>() {
            @Override
            protected boolean isIncluded(InMemoryEdge src, Edge edge) {
                String inVertexId = src.getVertexId(Direction.IN);
                checkNotNull(inVertexId, "inVertexId was null");
                String outVertexId = src.getVertexId(Direction.OUT);
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

    public void saveProperties(Element element, Iterable<Property> properties, IndexHint indexHint, Authorizations authorizations) {
        if (element instanceof Vertex) {
            InMemoryVertex vertex = vertices.get(element.getId());
            vertex.updatePropertiesInternal(properties);
        } else if (element instanceof Edge) {
            InMemoryEdge edge = edges.get(element.getId());
            edge.updatePropertiesInternal(properties);
        } else {
            throw new IllegalArgumentException("Unexpected element type: " + element.getClass().getName());
        }

        if (indexHint != IndexHint.DO_NOT_INDEX) {
            getSearchIndex().addElement(this, element, authorizations);
        }
    }

    public void removeProperty(Element element, Property property, Authorizations authorizations) {
        if (element instanceof Vertex) {
            InMemoryVertex vertex = vertices.get(element.getId());
            vertex.removePropertyInternal(property.getKey(), property.getName());
        } else if (element instanceof Edge) {
            InMemoryEdge edge = edges.get(element.getId());
            edge.removePropertyInternal(property.getKey(), property.getName());
        } else {
            throw new IllegalArgumentException("Unexpected element type: " + element.getClass().getName());
        }
        getSearchIndex().removeProperty(this, element, property, authorizations);

        if (hasEventListeners()) {
            fireGraphEvent(new RemovePropertyEvent(this, element, property));
        }
    }

    private Edge filteredEdge(InMemoryEdge edge, Authorizations authorizations) {
        String edgeId = edge.getId();
        String outVertexId = edge.getVertexId(Direction.OUT);
        String inVertexId = edge.getVertexId(Direction.IN);
        String label = edge.getLabel();
        Visibility visibility = edge.getVisibility();
        List<Property> properties = filterProperties(edge.getProperties(), authorizations);
        return new InMemoryEdge(this, edgeId, outVertexId, inVertexId, label, visibility, properties, authorizations);
    }

    private Vertex filteredVertex(InMemoryVertex vertex, Authorizations authorizations) {
        String vertexId = vertex.getId();
        Visibility visibility = vertex.getVisibility();
        List<Property> properties = filterProperties(vertex.getProperties(), authorizations);
        return new InMemoryVertex(this, vertexId, visibility, properties, authorizations);
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

    public Map<String, InMemoryVertex> getAllVertices() {
        return this.vertices;
    }

    public Map<String, InMemoryEdge> getAllEdges() {
        return this.edges;
    }

    void alterEdgeVisibility(String edgeId, Visibility newEdgeVisibility) {
        this.edges.get(edgeId).setVisibilityInternal(newEdgeVisibility);
    }

    void alterVertexVisibility(String vertexId, Visibility newVertexVisibility) {
        this.vertices.get(vertexId).setVisibilityInternal(newVertexVisibility);
    }

    void alterEdgePropertyVisibilities(String edgeId, List<AlterPropertyVisibility> alterPropertyVisibilities, Authorizations authorizations) {
        alterElementPropertyVisibilities(this.edges.get(edgeId), alterPropertyVisibilities, authorizations);
    }

    void alterVertexPropertyVisibilities(String vertexId, List<AlterPropertyVisibility> alterPropertyVisibilities, Authorizations authorizations) {
        alterElementPropertyVisibilities(this.vertices.get(vertexId), alterPropertyVisibilities, authorizations);
    }

    void alterElementPropertyVisibilities(InMemoryElement element, List<AlterPropertyVisibility> alterPropertyVisibilities, Authorizations authorizations) {
        for (AlterPropertyVisibility apv : alterPropertyVisibilities) {
            Property property = element.getProperty(apv.getKey(), apv.getName(), apv.getExistingVisibility());
            if (property == null) {
                throw new SecureGraphException("Could not find property " + apv.getKey() + ":" + apv.getName());
            }
            Object value = property.getValue();
            Map<String, Object> metadata = property.getMetadata();

            element.removeProperty(apv.getKey(), apv.getName(), authorizations);
            element.addPropertyValue(apv.getKey(), apv.getName(), value, metadata, apv.getVisibility(), authorizations);
        }
    }

    public void alterEdgePropertyMetadata(String edgeId, List<AlterPropertyMetadata> alterPropertyMetadatas) {
        alterElementPropertyMetadata(this.edges.get(edgeId), alterPropertyMetadatas);
    }

    public void alterVertexPropertyMetadata(String vertexId, List<AlterPropertyMetadata> alterPropertyMetadatas) {
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

    @Override
    public void clearData() {
        this.vertices.clear();
        this.edges.clear();
        getSearchIndex().clearData();
    }
}
