package com.altamiracorp.securegraph.query;

import com.altamiracorp.securegraph.Authorizations;
import com.altamiracorp.securegraph.Graph;

public abstract class GraphQueryBase extends QueryBase implements GraphQuery {
    protected GraphQueryBase(Graph graph, Authorizations authorizations) {
        super(graph, authorizations);
    }
}
