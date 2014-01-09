package com.altamiracorp.securegraph.query;

import com.altamiracorp.securegraph.*;
import com.altamiracorp.securegraph.util.FilterIterable;

public class DefaultVertexQuery extends VertexQueryBase implements VertexQuery {
    public DefaultVertexQuery(Graph graph, Vertex sourceVertex, String queryString, Authorizations authorizations) {
        super(graph, sourceVertex, queryString, authorizations);
    }

    @Override
    public Iterable<Edge> edges() {
        return new FilterIterable<Edge>(getGraph().getEdges(getParameters().getAuthorizations())) {
            @Override
            protected boolean isIncluded(Edge edge) {
                if (edge.getVertexId(Direction.OUT).equals(getSourceVertex().getId())) {
                    return true;
                }
                if (edge.getVertexId(Direction.IN).equals(getSourceVertex().getId())) {
                    return true;
                }
                return false;
            }
        };
    }
}
