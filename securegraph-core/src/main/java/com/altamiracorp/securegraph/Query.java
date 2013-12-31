package com.altamiracorp.securegraph;

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

    <T> Query has(String propertyName, T value);

    <T> Query has(String propertyName, Predicate predicate, T value);
}
