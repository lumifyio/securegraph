package org.securegraph.elasticsearch;

import org.securegraph.*;
import org.securegraph.mutation.ExistingElementMutation;

import java.io.Serializable;

public class ElasticSearchElement<T extends Element> extends ElementBase<T> implements Serializable {
    protected ElasticSearchElement(Graph graph, String id, Visibility visibility, Iterable<Property> properties, Authorizations authorizations) {
        super(graph, id, visibility, properties, authorizations);
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
}
