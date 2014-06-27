package org.securegraph.search;

import org.securegraph.*;
import org.securegraph.query.DefaultGraphQuery;
import org.securegraph.query.DefaultVertexQuery;
import org.securegraph.query.GraphQuery;
import org.securegraph.query.VertexQuery;

import java.util.HashMap;
import java.util.Map;

public class DefaultSearchIndex implements SearchIndex {
    private Map<String, PropertyDefinition> propertyDefinitions = new HashMap<String, PropertyDefinition>();

    public DefaultSearchIndex(Map configuration) {

    }

    @Override
    public void addElement(Graph graph, Element element, Authorizations authorizations) {

    }

    @Override
    public void removeElement(Graph graph, Element element, Authorizations authorizations) {

    }

    @Override
    public void removeProperty(Graph graph, Element element, Property property, Authorizations authorizations) {

    }

    @Override
    public void addElements(Graph graph, Iterable<Element> elements, Authorizations authorizations) {
        for (Element element : elements) {
            addElement(graph, element, authorizations);
        }
    }

    @Override
    public GraphQuery queryGraph(Graph graph, String queryString, Authorizations authorizations) {
        return new DefaultGraphQuery(graph, queryString, this.propertyDefinitions, authorizations);
    }

    @Override
    public VertexQuery queryVertex(Graph graph, Vertex vertex, String queryString, Authorizations authorizations) {
        return new DefaultVertexQuery(graph, vertex, queryString, this.propertyDefinitions, authorizations);
    }

    @Override
    public void flush() {

    }

    @Override
    public void shutdown() {

    }

    @Override
    public void addPropertyDefinition(PropertyDefinition propertyDefinition) {
        this.propertyDefinitions.put(propertyDefinition.getPropertyName(), propertyDefinition);
    }

    @Override
    public boolean isFieldBoostSupported() {
        return false;
    }

    @Override
    public boolean isEdgeBoostSupported() {
        return false;
    }

    @Override
    public void clearData() {

    }
}
