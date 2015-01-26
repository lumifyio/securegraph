package org.securegraph.sql;

import org.securegraph.Authorizations;
import org.securegraph.Property;

import java.util.Iterator;

public class LazyProperties implements Iterable<Property> {
    private final SqlGraph sqlGraph;
    private final String vertexId;
    private final String vertexVisibility;
    private final Authorizations authorizations;

    public LazyProperties(SqlGraph sqlGraph, String vertexId, String vertexVisibility, Authorizations authorizations) {
        this.sqlGraph = sqlGraph;
        this.vertexId = vertexId;
        this.vertexVisibility = vertexVisibility;
        this.authorizations = authorizations;
    }

    @Override
    public Iterator<Property> iterator() {
        return this.sqlGraph.getVertexProperties(getVertexId(), getVertexVisibility(), getAuthorizations());
    }

    public String getVertexId() {
        return vertexId;
    }

    public String getVertexVisibility() {
        return vertexVisibility;
    }

    public Authorizations getAuthorizations() {
        return authorizations;
    }
}
