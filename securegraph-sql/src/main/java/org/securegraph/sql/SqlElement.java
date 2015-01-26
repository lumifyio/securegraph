package org.securegraph.sql;

import org.securegraph.*;
import org.securegraph.mutation.ExistingElementMutation;

import java.io.Serializable;

public class SqlElement<T extends Element> extends ElementBase<T> implements Serializable {
    private static final long serialVersionUID = 1L;

    protected SqlElement(SqlGraph graph, String id, Visibility visibility, Iterable<Property> properties, Iterable<Visibility> hiddenVisibilities, Authorizations authorizations) {
        super(graph, id, visibility, properties, hiddenVisibilities, authorizations);
    }

    @Override
    public SqlGraph getGraph() {
        return (SqlGraph) super.getGraph();
    }

    @Override
    public ExistingElementMutation<T> prepareMutation() {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void removeProperty(String key, String name, Authorizations authorizations) {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void removeProperty(String name, Authorizations authorizations) {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void markPropertyHidden(Property property, Visibility visibility, Authorizations authorizations) {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void markPropertyVisible(Property property, Visibility visibility, Authorizations authorizations) {
        throw new RuntimeException("not implemented");
    }
}
