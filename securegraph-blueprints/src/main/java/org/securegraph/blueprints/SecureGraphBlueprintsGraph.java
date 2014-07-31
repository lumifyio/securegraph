package org.securegraph.blueprints;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Features;
import com.tinkerpop.blueprints.GraphQuery;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.DefaultGraphQuery;
import org.securegraph.Authorizations;
import org.securegraph.Graph;
import org.securegraph.Visibility;
import org.securegraph.query.Compare;
import org.securegraph.util.ConvertingIterable;

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
        Visibility visibility = getVisibilityProvider().getVisibilityForVertex(SecureGraphBlueprintsConvert.idToString(id));
        Authorizations authorizations = getAuthorizationsProvider().getAuthorizations();
        return SecureGraphBlueprintsVertex.create(this, getSecureGraph().addVertex(SecureGraphBlueprintsConvert.idToString(id), visibility, authorizations), authorizations);
    }

    @Override
    public Vertex getVertex(Object id) {
        if (id == null) {
            throw new IllegalArgumentException("Id cannot be null");
        }
        Authorizations authorizations = getAuthorizationsProvider().getAuthorizations();
        return SecureGraphBlueprintsVertex.create(this, getSecureGraph().getVertex(SecureGraphBlueprintsConvert.idToString(id), authorizations), authorizations);
    }

    @Override
    public void removeVertex(Vertex vertex) {
        org.securegraph.Vertex sgVertex = SecureGraphBlueprintsConvert.toSecureGraph(vertex);
        getSecureGraph().removeVertex(sgVertex, getAuthorizationsProvider().getAuthorizations());
    }

    @Override
    public Iterable<Vertex> getVertices() {
        final Authorizations authorizations = getAuthorizationsProvider().getAuthorizations();
        return new ConvertingIterable<org.securegraph.Vertex, Vertex>(getSecureGraph().getVertices(authorizations)) {
            @Override
            protected Vertex convert(org.securegraph.Vertex vertex) {
                return SecureGraphBlueprintsVertex.create(SecureGraphBlueprintsGraph.this, vertex, authorizations);
            }
        };
    }

    @Override
    public Iterable<Vertex> getVertices(final String key, final Object value) {
        final Authorizations authorizations = getAuthorizationsProvider().getAuthorizations();
        return new ConvertingIterable<org.securegraph.Vertex, Vertex>(getSecureGraph().query(authorizations).has(key, Compare.EQUAL, value).vertices()) {
            @Override
            protected Vertex convert(org.securegraph.Vertex vertex) {
                return SecureGraphBlueprintsVertex.create(SecureGraphBlueprintsGraph.this, vertex, authorizations);
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
        Visibility visibility = getVisibilityProvider().getVisibilityForEdge(SecureGraphBlueprintsConvert.idToString(id), sgOutVertex, sgInVertex, label);
        Authorizations authorizations = getAuthorizationsProvider().getAuthorizations();
        return SecureGraphBlueprintsEdge.create(this, getSecureGraph().addEdge(SecureGraphBlueprintsConvert.idToString(id), sgOutVertex, sgInVertex, label, visibility, authorizations), authorizations);
    }

    @Override
    public Edge getEdge(Object id) {
        if (id == null) {
            throw new IllegalArgumentException("Id cannot be null");
        }
        Authorizations authorizations = getAuthorizationsProvider().getAuthorizations();
        return SecureGraphBlueprintsEdge.create(this, getSecureGraph().getEdge(SecureGraphBlueprintsConvert.idToString(id), authorizations), authorizations);
    }

    @Override
    public void removeEdge(Edge edge) {
        org.securegraph.Edge sgEdge = SecureGraphBlueprintsConvert.toSecureGraph(edge);
        getSecureGraph().removeEdge(sgEdge, getAuthorizationsProvider().getAuthorizations());
    }

    @Override
    public Iterable<Edge> getEdges() {
        final Authorizations authorizations = getAuthorizationsProvider().getAuthorizations();
        return new ConvertingIterable<org.securegraph.Edge, Edge>(getSecureGraph().getEdges(authorizations)) {
            @Override
            protected Edge convert(org.securegraph.Edge edge) {
                return SecureGraphBlueprintsEdge.create(SecureGraphBlueprintsGraph.this, edge, authorizations);
            }
        };
    }

    @Override
    public Iterable<Edge> getEdges(final String key, final Object value) {
        final Authorizations authorizations = getAuthorizationsProvider().getAuthorizations();
        return new ConvertingIterable<org.securegraph.Edge, Edge>(getSecureGraph().query(authorizations).has(key, Compare.EQUAL, value).edges()) {
            @Override
            protected Edge convert(org.securegraph.Edge edge) {
                return SecureGraphBlueprintsEdge.create(SecureGraphBlueprintsGraph.this, edge, authorizations);
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
