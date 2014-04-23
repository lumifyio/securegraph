package org.securegraph.query;

import org.securegraph.Authorizations;
import org.securegraph.Graph;

public abstract class GraphQueryBase extends QueryBase implements GraphQuery {
    protected GraphQueryBase(Graph graph, String queryString, Authorizations authorizations) {
        super(graph, queryString, authorizations);
    }
}
