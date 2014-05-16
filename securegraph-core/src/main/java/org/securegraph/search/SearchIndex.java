package org.securegraph.search;

import org.securegraph.*;
import org.securegraph.query.GraphQuery;
import org.securegraph.query.VertexQuery;

public interface SearchIndex {
    void addElement(Graph graph, Element element);

    void removeElement(Graph graph, Element element);

    void addElements(Graph graph, Iterable<Element> elements);

    GraphQuery queryGraph(Graph graph, String queryString, Authorizations authorizations);

    VertexQuery queryVertex(Graph graph, Vertex vertex, String queryString, Authorizations authorizations);

    void flush();

    void shutdown();

    void addPropertyDefinition(PropertyDefinition propertyDefinition);
}
