package com.altamiracorp.securegraph;

import com.altamiracorp.securegraph.id.IdGenerator;
import com.altamiracorp.securegraph.path.PathFindingAlgorithm;
import com.altamiracorp.securegraph.path.RecursivePathFindingAlgorithm;
import com.altamiracorp.securegraph.query.GraphQuery;
import com.altamiracorp.securegraph.search.SearchIndex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public abstract class GraphBase implements Graph {
    private static final Logger LOGGER = LoggerFactory.getLogger(GraphBase.class);
    private final GraphConfiguration configuration;
    private final IdGenerator idGenerator;
    private final SearchIndex searchIndex;
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
    public abstract Iterable<Vertex> getVertices(Authorizations authorizations) throws SecureGraphException;

    @Override
    public abstract void removeVertex(Vertex vertex, Authorizations authorizations);

    @Override
    public void removeVertex(String vertexId, Authorizations authorizations) {
        Vertex vertex = getVertex(vertexId, authorizations);
        if (vertex == null) {
            throw new IllegalArgumentException("Could not find vertex with id: " + vertexId);
        }
        removeVertex(vertex, authorizations);
    }

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
    public abstract Iterable<Edge> getEdges(Authorizations authorizations);

    @Override
    public abstract void removeEdge(Edge edge, Authorizations authorizations);

    @Override
    public Iterable<List<Object>> findPaths(Vertex sourceVertex, Vertex destVertex, int maxHops, Authorizations authorizations) {
        return pathFindingAlgorithm.findPaths(this, sourceVertex, destVertex, maxHops, authorizations);
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
}
