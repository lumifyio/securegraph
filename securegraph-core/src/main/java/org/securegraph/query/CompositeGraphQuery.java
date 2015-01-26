package org.securegraph.query;

import org.securegraph.Edge;
import org.securegraph.FetchHint;
import org.securegraph.Vertex;
import org.securegraph.util.SelectManyIterable;

import java.util.*;

public class CompositeGraphQuery implements Query {
    private final List<Query> queries;

    public CompositeGraphQuery(Query... queries) {
        this(Arrays.asList(queries));
    }

    public CompositeGraphQuery(Collection<Query> queries) {
        this.queries = new ArrayList<>(queries);
    }

    @Override
    public Iterable<Vertex> vertices() {
        return new SelectManyIterable<Query, Vertex>(this.queries) {
            @Override
            public Iterable<Vertex> getIterable(Query query) {
                return query.vertices();
            }
        };
    }

    @Override
    public Iterable<Vertex> vertices(final EnumSet<FetchHint> fetchHints) {
        return new SelectManyIterable<Query, Vertex>(this.queries) {
            @Override
            public Iterable<Vertex> getIterable(Query query) {
                return query.vertices(fetchHints);
            }
        };
    }

    @Override
    public Iterable<Edge> edges() {
        return new SelectManyIterable<Query, Edge>(this.queries) {
            @Override
            public Iterable<Edge> getIterable(Query query) {
                return query.edges();
            }
        };
    }

    @Override
    public Iterable<Edge> edges(final EnumSet<FetchHint> fetchHints) {
        return new SelectManyIterable<Query, Edge>(this.queries) {
            @Override
            public Iterable<Edge> getIterable(Query query) {
                return query.edges(fetchHints);
            }
        };
    }

    @Override
    public Iterable<Edge> edges(final String label) {
        return new SelectManyIterable<Query, Edge>(this.queries) {
            @Override
            public Iterable<Edge> getIterable(Query query) {
                return query.edges(label);
            }
        };
    }

    @Override
    public Iterable<Edge> edges(final String label, final EnumSet<FetchHint> fetchHints) {
        return new SelectManyIterable<Query, Edge>(this.queries) {
            @Override
            public Iterable<Edge> getIterable(Query query) {
                return query.edges(label, fetchHints);
            }
        };
    }

    @Override
    public <T> Query range(String propertyName, T startValue, T endValue) {
        for (Query query : queries) {
            query.range(propertyName, startValue, endValue);
        }
        return this;
    }

    @Override
    public <T> Query has(String propertyName, T value) {
        for (Query query : queries) {
            query.has(propertyName, value);
        }
        return this;
    }

    @Override
    public <T> Query has(String propertyName, Predicate predicate, T value) {
        for (Query query : queries) {
            query.has(propertyName, predicate, value);
        }
        return this;
    }

    @Override
    public Query skip(int count) {
        for (Query query : queries) {
            query.skip(count);
        }
        return this;
    }

    @Override
    public Query limit(int count) {
        for (Query query : queries) {
            query.limit(count);
        }
        return this;
    }
}
