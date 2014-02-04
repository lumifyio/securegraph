package com.altamiracorp.securegraph.query;

import com.altamiracorp.securegraph.Edge;
import com.altamiracorp.securegraph.Vertex;

public interface Query {
    Iterable<Vertex> vertices();

    Iterable<Edge> edges();

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
     * Adds an {@link com.altamiracorp.securegraph.query.Compare#EQUAL} filter to the query.
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
     * @param predicate    One of {@link com.altamiracorp.securegraph.query.Compare},
     *                     {@link com.altamiracorp.securegraph.query.TextPredicate},
     *                     or {@link com.altamiracorp.securegraph.query.GeoCompare}.
     * @param value        The value of the property to query for.
     * @return The query object, allowing you to chain methods.
     */
    <T> Query has(String propertyName, Predicate predicate, T value);

    Query skip(int count);

    Query limit(int count);
}
