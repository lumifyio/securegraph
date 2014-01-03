package com.altamiracorp.securegraph.blueprints;

import com.altamiracorp.securegraph.util.ConvertingIterable;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.VertexQuery;
import com.tinkerpop.blueprints.util.DefaultVertexQuery;

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
    public Iterable<Edge> getEdges(Direction direction, String... labels) {
        // TODO filter by labels
        com.altamiracorp.securegraph.Direction sgDirection = SecureGraphBlueprintsConvert.toSecureGraph(direction);
        return new ConvertingIterable<com.altamiracorp.securegraph.Edge, Edge>(getSecureGraphElement().getEdges(sgDirection, getGraph().getAuthorizations())) {
            @Override
            protected Edge convert(com.altamiracorp.securegraph.Edge edge) {
                return SecureGraphBlueprintsEdge.create(getGraph(), edge);
            }
        };
    }

    @Override
    public Iterable<Vertex> getVertices(Direction direction, String... labels) {
        com.altamiracorp.securegraph.Direction sgDirection = SecureGraphBlueprintsConvert.toSecureGraph(direction);
        return new ConvertingIterable<com.altamiracorp.securegraph.Vertex, Vertex>(getSecureGraphElement().getVertices(sgDirection, getGraph().getAuthorizations())) {
            @Override
            protected Vertex convert(com.altamiracorp.securegraph.Vertex vertex) {
                return SecureGraphBlueprintsVertex.create(getGraph(), vertex);
            }
        };
    }

    @Override
    public VertexQuery query() {
        return new DefaultVertexQuery(this); // TODO implement
    }

    @Override
    public Edge addEdge(String label, Vertex inVertex) {
        throw new RuntimeException("not implemented");
    }

    @Override
    public com.altamiracorp.securegraph.Vertex getSecureGraphElement() {
        return (com.altamiracorp.securegraph.Vertex) super.getSecureGraphElement();
    }
}
