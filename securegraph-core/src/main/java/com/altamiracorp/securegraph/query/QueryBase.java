package com.altamiracorp.securegraph.query;

import com.altamiracorp.securegraph.*;
import com.altamiracorp.securegraph.util.FilterIterable;

import java.util.ArrayList;
import java.util.List;

public abstract class QueryBase implements Query {
    private final Graph graph;
    private final Parameters parameters;

    protected QueryBase(Graph graph, String queryString, Authorizations authorizations) {
        this.graph = graph;
        this.parameters = createParameters(queryString, authorizations);
    }

    protected Parameters createParameters(String queryString, Authorizations authorizations) {
        return new Parameters(queryString, authorizations);
    }

    @Override
    public abstract Iterable<Vertex> vertices();

    @Override
    public abstract Iterable<Edge> edges();

    @Override
    public Iterable<Edge> edges(final String label) {
        return new FilterIterable<Edge>(edges()) {
            @Override
            protected boolean isIncluded(Edge o) {
                return label.equals(o.getLabel());
            }
        };
    }

    @Override
    public <T> Query range(String propertyName, T startValue, T endValue) {
        this.parameters.addHasContainer(new HasContainer(propertyName, Compare.GREATER_THAN_EQUAL, startValue));
        this.parameters.addHasContainer(new HasContainer(propertyName, Compare.LESS_THAN_EQUAL, endValue));
        return this;
    }

    @Override
    public <T> Query has(String propertyName, T value) {
        this.parameters.addHasContainer(new HasContainer(propertyName, Compare.EQUAL, value));
        return this;
    }

    @Override
    public <T> Query has(String propertyName, Predicate predicate, T value) {
        this.parameters.addHasContainer(new HasContainer(propertyName, predicate, value));
        return this;
    }

    @Override
    public Query skip(int count) {
        this.parameters.setSkip(count);
        return this;
    }

    @Override
    public Query limit(int count) {
        this.parameters.setLimit(count);
        return this;
    }

    public Graph getGraph() {
        return graph;
    }

    public Parameters getParameters() {
        return parameters;
    }

    public static class HasContainer {
        public String key;
        public Object value;
        public Predicate predicate;

        public HasContainer(final String key, final Predicate predicate, final Object value) {
            this.key = key;
            this.value = value;
            this.predicate = predicate;
        }

        public boolean isMatch(Element elem) {
            return this.predicate.evaluate(elem.getProperties(this.key), this.value);
        }
    }

    public static class Parameters {
        private final Authorizations authorizations;
        private final String queryString;
        private long limit = 100;
        private long skip = 0;
        private final List<HasContainer> hasContainers = new ArrayList<HasContainer>();

        public Parameters(String queryString, Authorizations authorizations) {
            this.queryString = queryString;
            this.authorizations = authorizations;
        }

        public void addHasContainer(HasContainer hasContainer) {
            this.hasContainers.add(hasContainer);
        }

        public String getQueryString() {
            return queryString;
        }

        public long getLimit() {
            return limit;
        }

        public void setLimit(long limit) {
            this.limit = limit;
        }

        public long getSkip() {
            return skip;
        }

        public void setSkip(long skip) {
            this.skip = skip;
        }

        public Authorizations getAuthorizations() {
            return authorizations;
        }

        public List<HasContainer> getHasContainers() {
            return hasContainers;
        }
    }
}
