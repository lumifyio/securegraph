package com.altamiracorp.securegraph;

import com.altamiracorp.securegraph.query.VertexQuery;

public interface Vertex extends Element {
    /**
     * Gets all edges attached to this vertex.
     *
     * @param direction      The side of the edge that this vertex is attached to.
     * @param authorizations The authorizations used to find the edges.
     * @return An Iterable of edges.
     */
    Iterable<Edge> getEdges(Direction direction, Authorizations authorizations);

    Iterable<Object> getEdgeIds(Direction direction, Authorizations authorizations);

    /**
     * Gets all edges with the given label attached to this vertex.
     *
     * @param direction      The side of the edge that this vertex is attached to.
     * @param label          The edge label to search for.
     * @param authorizations The authorizations used to find the edges.
     * @return An Iterable of edges.
     */
    Iterable<Edge> getEdges(Direction direction, String label, Authorizations authorizations);

    Iterable<Object> getEdgeIds(Direction direction, String label, Authorizations authorizations);

    /**
     * Gets all edges with any of the given labels attached to this vertex.
     *
     * @param direction      The side of the edge that this vertex is attached to.
     * @param labels         An array of edge labels to search for.
     * @param authorizations The authorizations used to find the edges.
     * @return An Iterable of edges.
     */
    Iterable<Edge> getEdges(Direction direction, String[] labels, Authorizations authorizations);

    Iterable<Object> getEdgeIds(Direction direction, String[] labels, Authorizations authorizations);

    /**
     * Gets all edges between this vertex and another vertex.
     *
     * @param otherVertex    The other vertex.
     * @param direction      The direction of edges to find relative to this vertex.
     * @param authorizations The authorizations used to find the edges.
     * @return An Iterable of edges.
     */
    Iterable<Edge> getEdges(Vertex otherVertex, Direction direction, Authorizations authorizations);

    Iterable<Object> getEdgeIds(Vertex otherVertex, Direction direction, Authorizations authorizations);

    /**
     * Gets all edges between this vertex and another vertex matching the given label.
     *
     * @param otherVertex    The other vertex.
     * @param direction      The direction of edges to find relative to this vertex.
     * @param label          The edge label to search for.
     * @param authorizations The authorizations used to find the edges.
     * @return An Iterable of edges.
     */
    Iterable<Edge> getEdges(Vertex otherVertex, Direction direction, String label, Authorizations authorizations);

    Iterable<Object> getEdgeIds(Vertex otherVertex, Direction direction, String label, Authorizations authorizations);

    /**
     * Gets all edges between this vertex and another vertex matching any of the given labels.
     *
     * @param otherVertex    The other vertex.
     * @param direction      The direction of edges to find relative to this vertex.
     * @param labels         An array of edge labels to search for.
     * @param authorizations The authorizations used to find the edges.
     * @return An Iterable of edges.
     */
    Iterable<Edge> getEdges(Vertex otherVertex, Direction direction, String[] labels, Authorizations authorizations);

    Iterable<Object> getEdgeIds(Vertex otherVertex, Direction direction, String[] labels, Authorizations authorizations);

    /**
     * Similar to getEdges but gets the vertices on the other side of the edges attached to this vertex.
     *
     * @param direction      The direction relative to this vertex.
     * @param authorizations The authorizations used to find the vertices.
     * @return An Iterable of vertices.
     */
    Iterable<Vertex> getVertices(Direction direction, Authorizations authorizations);

    /**
     * Similar to getEdges but gets the vertices on the other side of the edges attached to this vertex that have the given label.
     *
     * @param direction      The direction relative to this vertex.
     * @param label          The label of edges to traverse to find the vertices.
     * @param authorizations The authorizations used to find the vertices.
     * @return An Iterable of vertices.
     */
    Iterable<Vertex> getVertices(Direction direction, String label, Authorizations authorizations);

    /**
     * Similar to getEdges but gets the vertices on the other side of the edges attached to this vertex that have any of the given labels.
     *
     * @param direction      The direction relative to this vertex.
     * @param labels         The labels of edges to traverse to find the vertices.
     * @param authorizations The authorizations used to find the vertices.
     * @return An Iterable of vertices.
     */
    Iterable<Vertex> getVertices(Direction direction, String[] labels, Authorizations authorizations);

    /**
     * Gets vertex ids of connected vertices.
     *
     * @param direction      The direction relative to this vertex.
     * @param label          The label of edges to traverse to find the vertices.
     * @param authorizations The authorizations used to find the vertices.
     * @return An Iterable of vertex ids.
     */
    Iterable<Object> getVertexIds(Direction direction, String label, Authorizations authorizations);

    /**
     * Gets vertex ids of connected vertices.
     *
     * @param direction      The direction relative to this vertex.
     * @param labels         The labels of edges to traverse to find the vertices.
     * @param authorizations The authorizations used to find the vertices.
     * @return An Iterable of vertex ids.
     */
    Iterable<Object> getVertexIds(Direction direction, String[] labels, Authorizations authorizations);

    /**
     * Gets vertex ids of connected vertices.
     *
     * @param direction      The direction relative to this vertex.
     * @param authorizations The authorizations used to find the vertices.
     * @return An Iterable of vertex ids.
     */
    Iterable<Object> getVertexIds(Direction direction, Authorizations authorizations);

    /**
     * Creates a query to query the edges and vertices attached to this vertex.
     *
     * @param authorizations The authorizations used to find the edges and vertices.
     * @return The query builder.
     */
    VertexQuery query(Authorizations authorizations);

    /**
     * Creates a query to query the edges and vertices attached to this vertex.
     *
     * @param queryString    The string to search for.
     * @param authorizations The authorizations used to find the edges and vertices.
     * @return The query builder.
     */
    VertexQuery query(String queryString, Authorizations authorizations);

    /**
     * Prepares a mutation to allow changing multiple property values at the same time. This method is similar to
     * com.altamiracorp.securegraph.Graph#prepareVertex(com.altamiracorp.securegraph.Visibility, com.altamiracorp.securegraph.Authorizations)
     * in that it allows multiple properties to be changed and saved in a single mutation.
     *
     * @return The mutation builder.
     */
    ElementMutation<Vertex> prepareMutation();
}
