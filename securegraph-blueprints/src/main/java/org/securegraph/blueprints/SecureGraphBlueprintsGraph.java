package org.securegraph.blueprints;

import org.securegraph.Graph;
import org.securegraph.Visibility;
import org.securegraph.query.Compare;
import org.securegraph.util.ConvertingIterable;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Features;
import com.tinkerpop.blueprints.GraphQuery;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.DefaultGraphQuery;

public abstract class SecureGraphBlueprintsGraph implements com.tinkerpop.blueprints.Graph {
    private static final SecureGraphBlueprintsGraphFeatures FEATURES = new SecureGraphBlueprintsGraphFeatures();
    private final Graph secureGraph;
    private final VisibilityProvider visibilityProvider;
    private final AuthorizationsProvider authorizationsProvider;

    protected SecureGraphBlueprintsGraph(Graph secureGraph, VisibilityProvider visibilityProvider, AuthorizationsProvider authorizationsProvider) {
        this.secureGraph = secureGraph;
        this.visibilityProvider = visibilityProvider;
        this.authorizationsProvider = authorizationsProvider;
    }

    @Override
    public Features getFeatures() {
        return FEATURES;
    }

    @Override
    public Vertex addVertex(Object id) {
        Visibility visibility = getVisibilityProvider().getVisibilityForVertex(id);
        return SecureGraphBlueprintsVertex.create(this, getSecureGraph().addVertex(SecureGraphBlueprintsConvert.idToString(id), visibility, getAuthorizationsProvider().getAuthorizations()));
    }

    @Override
    public Vertex getVertex(Object id) {
        if (id == null) {
            throw new IllegalArgumentException("Id cannot be null");
        }
        return SecureGraphBlueprintsVertex.create(this, getSecureGraph().getVertex(SecureGraphBlueprintsConvert.idToString(id), getAuthorizationsProvider().getAuthorizations()));
    }

    @Override
    public void removeVertex(Vertex vertex) {
        org.securegraph.Vertex sgVertex = SecureGraphBlueprintsConvert.toSecureGraph(vertex);
        getSecureGraph().removeVertex(sgVertex, getAuthorizationsProvider().getAuthorizations());
    }

    @Override
    public Iterable<Vertex> getVertices() {
        return new ConvertingIterable<org.securegraph.Vertex, Vertex>(getSecureGraph().getVertices(getAuthorizationsProvider().getAuthorizations())) {
            @Override
            protected Vertex convert(org.securegraph.Vertex vertex) {
                return SecureGraphBlueprintsVertex.create(SecureGraphBlueprintsGraph.this, vertex);
            }
        };
    }

    @Override
    public Iterable<Vertex> getVertices(final String key, final Object value) {
        return new ConvertingIterable<org.securegraph.Vertex, Vertex>(getSecureGraph().query(getAuthorizationsProvider().getAuthorizations()).has(key, Compare.EQUAL, value).vertices()) {
            @Override
            protected Vertex convert(org.securegraph.Vertex vertex) {
                return SecureGraphBlueprintsVertex.create(SecureGraphBlueprintsGraph.this, vertex);
            }
        };

    }

    @Override
    public Edge addEdge(Object id, Vertex outVertex, Vertex inVertex, String label) {
        if (label == null) {
            throw new IllegalArgumentException("label cannot be null");
        }
        org.securegraph.Vertex sgOutVertex = SecureGraphBlueprintsConvert.toSecureGraph(outVertex);
        org.securegraph.Vertex sgInVertex = SecureGraphBlueprintsConvert.toSecureGraph(inVertex);
        Visibility visibility = getVisibilityProvider().getVisibilityForEdge(id, sgOutVertex, sgInVertex, label);
        return SecureGraphBlueprintsEdge.create(this, getSecureGraph().addEdge(SecureGraphBlueprintsConvert.idToString(id), sgOutVertex, sgInVertex, label, visibility, getAuthorizationsProvider().getAuthorizations()));
    }

    @Override
    public Edge getEdge(Object id) {
        if (id == null) {
            throw new IllegalArgumentException("Id cannot be null");
        }
        return SecureGraphBlueprintsEdge.create(this, getSecureGraph().getEdge(SecureGraphBlueprintsConvert.idToString(id), getAuthorizationsProvider().getAuthorizations()));
    }

    @Override
    public void removeEdge(Edge edge) {
        org.securegraph.Edge sgEdge = SecureGraphBlueprintsConvert.toSecureGraph(edge);
        getSecureGraph().removeEdge(sgEdge, getAuthorizationsProvider().getAuthorizations());
    }

    @Override
    public Iterable<Edge> getEdges() {
        return new ConvertingIterable<org.securegraph.Edge, Edge>(getSecureGraph().getEdges(getAuthorizationsProvider().getAuthorizations())) {
            @Override
            protected Edge convert(org.securegraph.Edge edge) {
                return SecureGraphBlueprintsEdge.create(SecureGraphBlueprintsGraph.this, edge);
            }
        };
    }

    @Override
    public Iterable<Edge> getEdges(final String key, final Object value) {
        return new ConvertingIterable<org.securegraph.Edge, Edge>(getSecureGraph().query(getAuthorizationsProvider().getAuthorizations()).has(key, Compare.EQUAL, value).edges()) {
            @Override
            protected Edge convert(org.securegraph.Edge edge) {
                return SecureGraphBlueprintsEdge.create(SecureGraphBlueprintsGraph.this, edge);
            }
        };
    }

    @Override
    public GraphQuery query() {
        return new DefaultGraphQuery(this); // TODO implement this
    }

    @Override
    public void shutdown() {
        getSecureGraph().shutdown();
    }

    @Override
    public String toString() {
        return getClass().getSimpleName().toLowerCase();
    }

    public Graph getSecureGraph() {
        return secureGraph;
    }

    public VisibilityProvider getVisibilityProvider() {
        return visibilityProvider;
    }

    public AuthorizationsProvider getAuthorizationsProvider() {
        return authorizationsProvider;
    }
}
