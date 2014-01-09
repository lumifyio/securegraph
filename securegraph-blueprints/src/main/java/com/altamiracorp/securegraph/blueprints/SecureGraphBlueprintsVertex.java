package com.altamiracorp.securegraph.blueprints;

import com.altamiracorp.securegraph.util.LookAheadIterable;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.VertexQuery;
import com.tinkerpop.blueprints.util.DefaultVertexQuery;

import java.util.Iterator;

public class SecureGraphBlueprintsVertex extends SecureGraphBlueprintsElement implements Vertex {
    protected SecureGraphBlueprintsVertex(SecureGraphBlueprintsGraph graph, com.altamiracorp.securegraph.Vertex vertex) {
        super(graph, vertex);
    }

    public static Vertex create(SecureGraphBlueprintsGraph graph, com.altamiracorp.securegraph.Vertex vertex) {
        if (vertex == null) {
            return null;
        }
        return new SecureGraphBlueprintsVertex(graph, vertex);
    }

    @Override
    public Iterable<Edge> getEdges(Direction direction, final String... labels) {
        final com.altamiracorp.securegraph.Direction sgDirection = SecureGraphBlueprintsConvert.toSecureGraph(direction);
        return new LookAheadIterable<com.altamiracorp.securegraph.Edge, Edge>() {
            @Override
            protected boolean isIncluded(com.altamiracorp.securegraph.Edge src, Edge edge) {
                if (labels.length == 0) {
                    return true;
                }
                for (String label : labels) {
                    if (label.equals(edge.getLabel())) {
                        return true;
                    }
                }
                return false;
            }

            @Override
            protected Edge convert(com.altamiracorp.securegraph.Edge edge) {
                return SecureGraphBlueprintsEdge.create(getGraph(), edge);
            }

            @Override
            protected Iterator<com.altamiracorp.securegraph.Edge> createIterator() {
                return getSecureGraphElement().getEdges(sgDirection, getGraph().getAuthorizationsProvider().getAuthorizations()).iterator();
            }
        };
    }

    @Override
    public Iterable<Vertex> getVertices(Direction direction, final String... labels) {
        final com.altamiracorp.securegraph.Direction sgDirection = SecureGraphBlueprintsConvert.toSecureGraph(direction);
        return new LookAheadIterable<com.altamiracorp.securegraph.Edge, Vertex>() {
            @Override
            protected boolean isIncluded(com.altamiracorp.securegraph.Edge edge, Vertex vertex) {
                if (labels.length == 0) {
                    return true;
                }
                for (String label : labels) {
                    if (label.equals(edge.getLabel())) {
                        return true;
                    }
                }
                return false;
            }

            @Override
            protected Vertex convert(com.altamiracorp.securegraph.Edge edge) {
                com.altamiracorp.securegraph.Vertex vertex = edge.getOtherVertex(getId(), getGraph().getAuthorizationsProvider().getAuthorizations());
                return SecureGraphBlueprintsVertex.create(getGraph(), vertex);
            }

            @Override
            protected Iterator<com.altamiracorp.securegraph.Edge> createIterator() {
                return getSecureGraphElement().getEdges(sgDirection, getGraph().getAuthorizationsProvider().getAuthorizations()).iterator();
            }
        };
    }

    @Override
    public VertexQuery query() {
        return new DefaultVertexQuery(this); // TODO implement
    }

    @Override
    public Edge addEdge(String label, Vertex inVertex) {
        if (label == null) {
            throw new IllegalArgumentException("Cannot add edge with null label");
        }
        return getGraph().addEdge(null, this, inVertex, label);
    }

    @Override
    public void remove() {
        getGraph().removeVertex(this);
    }

    @Override
    public com.altamiracorp.securegraph.Vertex getSecureGraphElement() {
        return (com.altamiracorp.securegraph.Vertex) super.getSecureGraphElement();
    }
}
