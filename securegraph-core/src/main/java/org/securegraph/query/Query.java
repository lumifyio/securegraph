package org.securegraph.query;

import org.securegraph.Edge;
import org.securegraph.FetchHint;
import org.securegraph.Vertex;

import java.util.EnumSet;

public interface Query {
    Iterable<Vertex> vertices();

    Iterable<Vertex> vertices(EnumSet<FetchHint> fetchHints);

    Iterable<Edge> edges();

    Iterable<Edge> edges(EnumSet<FetchHint> fetchHints);

    Iterable<Edge> edges(String label);

    Iterable<Edge> edges(String label, EnumSet<FetchHint> fetchHints);

    /**
     * Queries for properties in the given range.
     *
     * @param propertyName Name of property.
     * @param startValue   Inclusive start value.
     * @param endValue     Inclusive end value.
     * @return this
     */
    <T> Query range(String propertyName, T startValue, T endValue);

    /**
     * Adds an {@link org.securegraph.query.Compare#EQUAL} filter to the query.
     *
     * @param propertyName The name of the property to query on.
     * @param value        The value of the property to query for.
     * @return The query object, allowing you to chain methods.
     */
    <T> Query has(String propertyName, T value);

    /**
     * Adds a filter to the query.
     *
     * @param propertyName The name of the property to query on.
     * @param predicate    One of {@link org.securegraph.query.Compare},
     *                     {@link org.securegraph.query.TextPredicate},
     *                     or {@link org.securegraph.query.GeoCompare}.
     * @param value        The value of the property to query for.
     * @return The query object, allowing you to chain methods.
     */
    <T> Query has(String propertyName, Predicate predicate, T value);

    Query skip(int count);

    Query limit(int count);
}
