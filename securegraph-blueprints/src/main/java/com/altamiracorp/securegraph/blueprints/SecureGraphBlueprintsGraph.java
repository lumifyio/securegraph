package com.altamiracorp.securegraph.blueprints;

import com.altamiracorp.securegraph.Authorizations;
import com.altamiracorp.securegraph.Graph;
import com.altamiracorp.securegraph.Visibility;
import com.altamiracorp.securegraph.util.ConvertingIterable;
import com.altamiracorp.securegraph.util.FilterIterable;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Features;
import com.tinkerpop.blueprints.GraphQuery;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.DefaultGraphQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class SecureGraphBlueprintsGraph implements com.tinkerpop.blueprints.Graph {
    private static final Logger LOGGER = LoggerFactory.getLogger(SecureGraphBlueprintsGraph.class);
    private static final SecureGraphBlueprintsGraphFeatures FEATURES = new SecureGraphBlueprintsGraphFeatures();
    private static final Visibility DEFAULT_VISIBILITY = new Visibility("");
    private static final Authorizations DEFAULT_AUTHORIZATIONS = new Authorizations();
    private final Graph secureGraph;

    protected SecureGraphBlueprintsGraph(Graph secureGraph) {
        this.secureGraph = secureGraph;
    }

    @Override
    public Features getFeatures() {
        return FEATURES;
    }

    @Override
    public Vertex addVertex(Object id) {
        return SecureGraphBlueprintsVertex.create(this, getSecureGraph().addVertex(SecureGraphBlueprintsConvert.idToString(id), getVisibility()));
    }

    @Override
    public Vertex getVertex(Object id) {
        if (id == null) {
            throw new IllegalArgumentException("Id cannot be null");
        }
        return SecureGraphBlueprintsVertex.create(this, getSecureGraph().getVertex(SecureGraphBlueprintsConvert.idToString(id), getAuthorizations()));
    }

    @Override
    public void removeVertex(Vertex vertex) {
        com.altamiracorp.securegraph.Vertex sgVertex = SecureGraphBlueprintsConvert.toSecureGraph(vertex);
        getSecureGraph().removeVertex(sgVertex, getAuthorizations());
    }

    @Override
    public Iterable<Vertex> getVertices() {
        return new ConvertingIterable<com.altamiracorp.securegraph.Vertex, Vertex>(getSecureGraph().getVertices(getAuthorizations())) {
            @Override
            protected Vertex convert(com.altamiracorp.securegraph.Vertex vertex) {
                return SecureGraphBlueprintsVertex.create(SecureGraphBlueprintsGraph.this, vertex);
            }
        };
    }

    @Override
    public Iterable<Vertex> getVertices(final String key, final Object value) {
        LOGGER.warn("performing iteration over all vertices. implement getVertices(String, Object).");
        return new FilterIterable<Vertex>(getVertices()) {
            @Override
            protected boolean isIncluded(Vertex src, Vertex dest) {
                Object p = dest.getProperty(key);
                if (p == null && value == null) {
                    return true;
                }
                if (p == null) {
                    return false;
                }
                return p.equals(value);
            }
        };

    }

    @Override
    public Edge addEdge(Object id, Vertex outVertex, Vertex inVertex, String label) {
        if (label == null) {
            throw new IllegalArgumentException("label cannot be null");
        }
        com.altamiracorp.securegraph.Vertex sgOutVertex = SecureGraphBlueprintsConvert.toSecureGraph(outVertex);
        com.altamiracorp.securegraph.Vertex sgInVertex = SecureGraphBlueprintsConvert.toSecureGraph(inVertex);
        return SecureGraphBlueprintsEdge.create(this, getSecureGraph().addEdge(SecureGraphBlueprintsConvert.idToString(id), sgOutVertex, sgInVertex, label, getVisibility()));
    }

    @Override
    public Edge getEdge(Object id) {
        if (id == null) {
            throw new IllegalArgumentException("Id cannot be null");
        }
        return SecureGraphBlueprintsEdge.create(this, getSecureGraph().getEdge(SecureGraphBlueprintsConvert.idToString(id), getAuthorizations()));
    }

    @Override
    public void removeEdge(Edge edge) {
        com.altamiracorp.securegraph.Edge sgEdge = SecureGraphBlueprintsConvert.toSecureGraph(edge);
        getSecureGraph().removeEdge(sgEdge, getAuthorizations());
    }

    @Override
    public Iterable<Edge> getEdges() {
        return new ConvertingIterable<com.altamiracorp.securegraph.Edge, Edge>(getSecureGraph().getEdges(getAuthorizations())) {
            @Override
            protected Edge convert(com.altamiracorp.securegraph.Edge edge) {
                return SecureGraphBlueprintsEdge.create(SecureGraphBlueprintsGraph.this, edge);
            }
        };
    }

    @Override
    public Iterable<Edge> getEdges(final String key, final Object value) {
        LOGGER.warn("performing iteration over all edges. implement getEdges(String, Object).");
        return new FilterIterable<Edge>(getEdges()) {
            @Override
            protected boolean isIncluded(Edge src, Edge dest) {
                Object p = dest.getProperty(key);
                if (p == null && value == null) {
                    return true;
                }
                if (p == null) {
                    return false;
                }
                return p.equals(value);
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

    Visibility getVisibility() {
        return DEFAULT_VISIBILITY; // TODO should we load this from thread local or something
    }

    Authorizations getAuthorizations() {
        return DEFAULT_AUTHORIZATIONS; // TODO should we load this from thread local or something
    }
}
