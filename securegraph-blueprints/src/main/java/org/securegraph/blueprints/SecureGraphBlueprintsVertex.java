package org.securegraph.blueprints;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.VertexQuery;
import com.tinkerpop.blueprints.util.DefaultVertexQuery;
import org.securegraph.Authorizations;
import org.securegraph.util.ConvertingIterable;

public class SecureGraphBlueprintsVertex extends SecureGraphBlueprintsElement implements Vertex {
    protected SecureGraphBlueprintsVertex(SecureGraphBlueprintsGraph graph, org.securegraph.Vertex vertex, Authorizations authorizations) {
        super(graph, vertex, authorizations);
    }

    public static Vertex create(SecureGraphBlueprintsGraph graph, org.securegraph.Vertex vertex, Authorizations authorizations) {
        if (vertex == null) {
            return null;
        }
        return new SecureGraphBlueprintsVertex(graph, vertex, authorizations);
    }

    @Override
    public Iterable<Edge> getEdges(Direction direction, final String... labels) {
        final org.securegraph.Direction sgDirection = SecureGraphBlueprintsConvert.toSecureGraph(direction);
        final Authorizations authorizations = getGraph().getAuthorizationsProvider().getAuthorizations();
        return new ConvertingIterable<org.securegraph.Edge, Edge>(getSecureGraphElement().getEdges(sgDirection, labels, authorizations)) {
            @Override
            protected Edge convert(org.securegraph.Edge edge) {
                return SecureGraphBlueprintsEdge.create(getGraph(), edge, authorizations);
            }
        };
    }

    @Override
    public Iterable<Vertex> getVertices(Direction direction, final String... labels) {
        final org.securegraph.Direction sgDirection = SecureGraphBlueprintsConvert.toSecureGraph(direction);
        final Authorizations authorizations = getGraph().getAuthorizationsProvider().getAuthorizations();
        return new ConvertingIterable<org.securegraph.Vertex, Vertex>(getSecureGraphElement().getVertices(sgDirection, labels, authorizations)) {
            @Override
            protected Vertex convert(org.securegraph.Vertex vertex) {
                return SecureGraphBlueprintsVertex.create(getGraph(), vertex, authorizations);
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
    public org.securegraph.Vertex getSecureGraphElement() {
        return (org.securegraph.Vertex) super.getSecureGraphElement();
    }
}
