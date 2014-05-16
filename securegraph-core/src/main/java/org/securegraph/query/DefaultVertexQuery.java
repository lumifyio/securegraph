package org.securegraph.query;

import org.securegraph.*;

import java.util.Map;

public class DefaultVertexQuery extends VertexQueryBase implements VertexQuery {
    public DefaultVertexQuery(Graph graph, Vertex sourceVertex, String queryString, Map<String, PropertyDefinition> propertyDefinitions, Authorizations authorizations) {
        super(graph, sourceVertex, queryString, propertyDefinitions, authorizations);
    }

    @Override
    public Iterable<Vertex> vertices() {
        return new DefaultGraphQueryIterable<Vertex>(getParameters(), getSourceVertex().getVertices(Direction.BOTH, getParameters().getAuthorizations()), true);
    }

    @Override
    public Iterable<Edge> edges() {
        return new DefaultGraphQueryIterable<Edge>(getParameters(), getSourceVertex().getEdges(Direction.BOTH, getParameters().getAuthorizations()), true);
    }


}
