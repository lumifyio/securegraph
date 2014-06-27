package org.securegraph.search;

import org.securegraph.*;
import org.securegraph.query.GraphQuery;
import org.securegraph.query.VertexQuery;

import java.io.IOException;

public interface SearchIndex {
    void addElement(Graph graph, Element element, Authorizations authorizations);

    void removeElement(Graph graph, Element element, Authorizations authorizations);

    void removeProperty(Graph graph, Element element, Property property, Authorizations authorizations);

    void addElements(Graph graph, Iterable<Element> elements, Authorizations authorizations);

    GraphQuery queryGraph(Graph graph, String queryString, Authorizations authorizations);

    VertexQuery queryVertex(Graph graph, Vertex vertex, String queryString, Authorizations authorizations);

    void flush();

    void shutdown();

    void addPropertyDefinition(PropertyDefinition propertyDefinition) throws IOException;

    boolean isFieldBoostSupported();

    boolean isEdgeBoostSupported();

    void clearData();
}
