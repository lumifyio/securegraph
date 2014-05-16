package org.securegraph;

import org.securegraph.id.IdGenerator;
import org.securegraph.path.PathFindingAlgorithm;
import org.securegraph.path.RecursivePathFindingAlgorithm;
import org.securegraph.query.GraphQuery;
import org.securegraph.search.SearchIndex;
import org.securegraph.util.LookAheadIterable;
import org.securegraph.util.ToElementIterable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.securegraph.util.IterableUtils.toList;

public abstract class GraphBase implements Graph {
    private static final Logger LOGGER = LoggerFactory.getLogger(GraphBase.class);
    private final GraphConfiguration configuration;
    private final IdGenerator idGenerator;
    private SearchIndex searchIndex;
    private final PathFindingAlgorithm pathFindingAlgorithm = new RecursivePathFindingAlgorithm();

    protected GraphBase(GraphConfiguration configuration, IdGenerator idGenerator, SearchIndex searchIndex) {
        this.configuration = configuration;
        this.idGenerator = idGenerator;
        this.searchIndex = searchIndex;
    }

    @Override
    public Vertex addVertex(Visibility visibility, Authorizations authorizations) {
        return prepareVertex(visibility, authorizations).save();
    }

    @Override
    public Vertex addVertex(Object vertexId, Visibility visibility, Authorizations authorizations) {
        return prepareVertex(vertexId, visibility, authorizations).save();
    }

    @Override
    public VertexBuilder prepareVertex(Visibility visibility, Authorizations authorizations) {
        return prepareVertex(getIdGenerator().nextId(), visibility, authorizations);
    }

    @Override
    public Vertex getVertex(Object vertexId, Authorizations authorizations) throws SecureGraphException {
        LOGGER.warn("Performing scan of all vertices! Override getVertex.");
        for (Vertex vertex : getVertices(authorizations)) {
            if (vertex.getId().equals(vertexId)) {
                return vertex;
            }
        }
        return null;
    }

    @Override
    public Iterable<Vertex> getVertices(final Iterable<Object> ids, final Authorizations authorizations) {
        LOGGER.warn("Getting each vertex one by one! Override getVertices(java.lang.Iterable<java.lang.Object>, org.securegraph.Authorizations)");
        return new LookAheadIterable<Object, Vertex>() {
            @Override
            protected boolean isIncluded(Object src, Vertex vertex) {
                return vertex != null;
            }

            @Override
            protected Vertex convert(Object id) {
                return getVertex(id, authorizations);
            }

            @Override
            protected Iterator<Object> createIterator() {
                return ids.iterator();
            }
        };
    }

    @Override
    public List<Vertex> getVerticesInOrder(Iterable<Object> ids, Authorizations authorizations) {
        final List<Object> vertexIds = toList(ids);
        List<Vertex> vertices = toList(getVertices(vertexIds, authorizations));
        Collections.sort(vertices, new Comparator<Vertex>() {
            @Override
            public int compare(Vertex v1, Vertex v2) {
                Integer i1 = vertexIds.indexOf(v1.getId());
                Integer i2 = vertexIds.indexOf(v2.getId());
                return i1.compareTo(i2);
            }
        });
        return vertices;
    }

    @Override
    public abstract Iterable<Vertex> getVertices(Authorizations authorizations) throws SecureGraphException;

    @Override
    public abstract void removeVertex(Vertex vertex, Authorizations authorizations);

    @Override
    public Edge addEdge(Vertex outVertex, Vertex inVertex, String label, Visibility visibility, Authorizations authorizations) {
        return prepareEdge(outVertex, inVertex, label, visibility, authorizations).save();
    }

    @Override
    public Edge addEdge(Object edgeId, Vertex outVertex, Vertex inVertex, String label, Visibility visibility, Authorizations authorizations) {
        return prepareEdge(edgeId, outVertex, inVertex, label, visibility, authorizations).save();
    }

    @Override
    public EdgeBuilder prepareEdge(Vertex outVertex, Vertex inVertex, String label, Visibility visibility, Authorizations authorizations) {
        return prepareEdge(getIdGenerator().nextId(), outVertex, inVertex, label, visibility, authorizations);
    }

    @Override
    public Edge getEdge(Object edgeId, Authorizations authorizations) {
        LOGGER.warn("Performing scan of all edges! Override getEdge.");
        for (Edge edge : getEdges(authorizations)) {
            if (edge.getId().equals(edgeId)) {
                return edge;
            }
        }
        return null;
    }

    @Override
    public Iterable<Edge> getEdges(final Iterable<Object> ids, final Authorizations authorizations) {
        LOGGER.warn("Getting each edge one by one! Override getEdges(java.lang.Iterable<java.lang.Object>, org.securegraph.Authorizations)");
        return new LookAheadIterable<Object, Edge>() {
            @Override
            protected boolean isIncluded(Object src, Edge edge) {
                return edge != null;
            }

            @Override
            protected Edge convert(Object id) {
                return getEdge(id, authorizations);
            }

            @Override
            protected Iterator<Object> createIterator() {
                return ids.iterator();
            }
        };
    }

    @Override
    public abstract Iterable<Edge> getEdges(Authorizations authorizations);

    @Override
    public abstract void removeEdge(Edge edge, Authorizations authorizations);

    @Override
    public Iterable<Path> findPaths(Vertex sourceVertex, Vertex destVertex, int maxHops, Authorizations authorizations) {
        return pathFindingAlgorithm.findPaths(this, sourceVertex, destVertex, maxHops, authorizations);
    }

    @Override
    public Iterable<Object> findRelatedEdges(Iterable<Object> vertexIds, Authorizations authorizations) {
        Set<Object> results = new HashSet<Object>();
        List<Vertex> vertices = toList(getVertices(vertexIds, authorizations));

        // since we are checking bi-directional edges we should only have to check v1->v2 and not v2->v1
        Map<String, String> checkedCombinations = new HashMap<String, String>();

        for (Vertex sourceVertex : vertices) {
            for (Vertex destVertex : vertices) {
                if (checkedCombinations.containsKey(sourceVertex.getId().toString() + destVertex.getId().toString())) {
                    continue;
                }
                Iterable<Object> edgeIds = sourceVertex.getEdgeIds(destVertex, Direction.BOTH, authorizations);
                for (Object edgeId : edgeIds) {
                    results.add(edgeId);
                }
                checkedCombinations.put(sourceVertex.getId().toString() + destVertex.getId().toString(), "");
                checkedCombinations.put(destVertex.getId().toString() + sourceVertex.getId().toString(), "");
            }
        }
        return results;
    }

    @Override
    public void removeEdge(String edgeId, Authorizations authorizations) {
        Edge edge = getEdge(edgeId, authorizations);
        if (edge == null) {
            throw new IllegalArgumentException("Could not find edge with id: " + edgeId);
        }
        removeEdge(edge, authorizations);
    }

    @Override
    public GraphQuery query(Authorizations authorizations) {
        return getSearchIndex().queryGraph(this, null, authorizations);
    }

    @Override
    public GraphQuery query(String queryString, Authorizations authorizations) {
        return getSearchIndex().queryGraph(this, queryString, authorizations);
    }

    public IdGenerator getIdGenerator() {
        return idGenerator;
    }

    public GraphConfiguration getConfiguration() {
        return configuration;
    }

    public SearchIndex getSearchIndex() {
        return searchIndex;
    }

    @Override
    public void reindex(Authorizations authorizations) {
        reindexVertices(authorizations);
        reindexEdges(authorizations);
    }

    protected void reindexVertices(Authorizations authorizations) {
        this.searchIndex.addElements(this, new ToElementIterable<Vertex>(getVertices(authorizations)));
    }

    private void reindexEdges(Authorizations authorizations) {
        this.searchIndex.addElements(this, new ToElementIterable<Edge>(getEdges(authorizations)));
    }

    @Override
    public void flush() {
        if (getSearchIndex() != null) {
            this.searchIndex.flush();
        }
    }

    @Override
    public void shutdown() {
        flush();
        if (getSearchIndex() != null) {
            this.searchIndex.shutdown();
            this.searchIndex = null;
        }
    }

    @Override
    public DefinePropertyBuilder defineProperty(String propertyName) {
        return new DefinePropertyBuilder(propertyName) {
            @Override
            public PropertyDefinition define() {
                PropertyDefinition propertyDefinition = super.define();
                getSearchIndex().addPropertyDefinition(propertyDefinition);
                return propertyDefinition;
            }
        };
    }
}
