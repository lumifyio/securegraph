package org.securegraph.query;

import org.securegraph.*;
import org.securegraph.util.FilterIterable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public abstract class QueryBase implements Query {
    private final Graph graph;
    private final Map<String, PropertyDefinition> propertyDefinitions;
    private final Parameters parameters;

    protected QueryBase(Graph graph, String queryString, Map<String, PropertyDefinition> propertyDefinitions, Authorizations authorizations) {
        this.graph = graph;
        this.propertyDefinitions = propertyDefinitions;
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
        this.parameters.addHasContainer(new HasContainer(propertyName, Compare.GREATER_THAN_EQUAL, startValue, this.propertyDefinitions));
        this.parameters.addHasContainer(new HasContainer(propertyName, Compare.LESS_THAN_EQUAL, endValue, this.propertyDefinitions));
        return this;
    }

    @Override
    public <T> Query has(String propertyName, T value) {
        this.parameters.addHasContainer(new HasContainer(propertyName, Compare.EQUAL, value, this.propertyDefinitions));
        return this;
    }

    @Override
    public <T> Query has(String propertyName, Predicate predicate, T value) {
        this.parameters.addHasContainer(new HasContainer(propertyName, predicate, value, this.propertyDefinitions));
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
        private final Map<String, PropertyDefinition> propertyDefinitions;

        public HasContainer(final String key, final Predicate predicate, final Object value, Map<String, PropertyDefinition> propertyDefinitions) {
            this.key = key;
            this.value = value;
            this.predicate = predicate;
            this.propertyDefinitions = propertyDefinitions;
        }

        public boolean isMatch(Element elem) {
            return this.predicate.evaluate(elem.getProperties(this.key), this.value, this.propertyDefinitions);
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

        public Parameters clone() {
            Parameters result = new Parameters(this.getQueryString(), this.getAuthorizations());
            result.setSkip(this.getSkip());
            result.setLimit(this.getLimit());
            result.hasContainers.addAll(this.getHasContainers());
            return result;
        }
    }
}
