package com.altamiracorp.securegraph;

import com.altamiracorp.securegraph.util.ConvertingIterable;
import com.altamiracorp.securegraph.util.FilterIterable;

public abstract class ElementBase implements Element {
    private final Graph graph;
    private final Object id;
    private final Visibility visibility;

    protected ElementBase(Graph graph, Object id, Visibility visibility) {
        this.graph = graph;
        this.id = id;
        this.visibility = visibility;
    }

    @Override
    public Iterable<Object> getPropertyValues(final String name) {
        return new ConvertingIterable<Property, Object>(getProperties(name)) {
            @Override
            protected Object convert(Property p) {
                return p.getValue();
            }
        };
    }

    @Override
    public Object getId() {
        return this.id;
    }

    @Override
    public Visibility getVisibility() {
        return this.visibility;
    }

    @Override
    public abstract Iterable<Property> getProperties();

    @Override
    public Iterable<Property> getProperties(final String name) {
        return new FilterIterable<Property>(getProperties()) {
            @Override
            protected boolean isIncluded(Property obj) {
                return obj.getName().equals(name);
            }
        };
    }

    @Override
    public abstract void setProperties(Property... properties);

    @Override
    public abstract void addProperties(Property... properties);

    public Graph getGraph() {
        return graph;
    }

    @Override
    public int hashCode() {
        return getId().hashCode();
    }

    @Override
    public String toString() {
        if (this instanceof Edge) {
            return getId() + ":[" + ((Edge) this).getOutVertexId() + "->" + ((Edge) this).getInVertexId() + "]";
        }
        return getId().toString();
    }
}
