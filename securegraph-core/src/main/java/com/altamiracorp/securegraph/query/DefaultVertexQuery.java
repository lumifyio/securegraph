package com.altamiracorp.securegraph.query;

import com.altamiracorp.securegraph.Authorizations;
import com.altamiracorp.securegraph.Edge;
import com.altamiracorp.securegraph.Graph;
import com.altamiracorp.securegraph.Vertex;

public class DefaultVertexQuery extends VertexQueryBase implements VertexQuery {
    public DefaultVertexQuery(Graph graph, Authorizations authorizations, Vertex sourceVertex) {
        super(graph, authorizations, sourceVertex);
    }

    @Override
    public Iterable<Edge> edges() {
        return null;
    }
}
