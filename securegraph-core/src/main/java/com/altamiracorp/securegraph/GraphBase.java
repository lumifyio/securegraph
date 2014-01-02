package com.altamiracorp.securegraph;

import com.altamiracorp.securegraph.id.IdGenerator;
import com.altamiracorp.securegraph.query.DefaultGraphQuery;
import com.altamiracorp.securegraph.query.GraphQuery;

public abstract class GraphBase implements Graph {
    private final GraphConfiguration configuration;
    private final IdGenerator idGenerator;

    protected GraphBase(GraphConfiguration configuration, IdGenerator idGenerator) {
        this.configuration = configuration;
        this.idGenerator = idGenerator;
    }

    @Override
    public Vertex addVertex(Visibility visibility, Property... properties) {
        return addVertex(getIdGenerator().nextId(), visibility, properties);
    }

    @Override
    public abstract Vertex addVertex(Object vertexId, Visibility visibility, Property... properties);

    @Override
    public Vertex getVertex(Object vertexId, Authorizations authorizations) throws SecureGraphException {
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
    public abstract void removeVertex(Object vertexId, Authorizations authorizations);

    @Override
    public Edge addEdge(Vertex outVertex, Vertex inVertex, String label, Visibility visibility, Property... properties) {
        return addEdge(getIdGenerator().nextId(), outVertex, inVertex, label, visibility, properties);
    }

    @Override
    public abstract Edge addEdge(Object edgeId, Vertex outVertex, Vertex inVertex, String label, Visibility visibility, Property... properties);

    @Override
    public Edge getEdge(Object edgeId, Authorizations authorizations) {
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
    public abstract void removeEdge(Object edgeId, Authorizations authorizations);

    @Override
    public GraphQuery query(Authorizations authorizations) {
        return new DefaultGraphQuery(this, authorizations);
    }

    public IdGenerator getIdGenerator() {
        return idGenerator;
    }

    public GraphConfiguration getConfiguration() {
        return configuration;
    }
}
