package org.securegraph.search;

import org.securegraph.Authorizations;
import org.securegraph.Element;
import org.securegraph.Graph;
import org.securegraph.Vertex;
import org.securegraph.query.DefaultGraphQuery;
import org.securegraph.query.DefaultVertexQuery;
import org.securegraph.query.GraphQuery;
import org.securegraph.query.VertexQuery;

import java.util.Map;

public class DefaultSearchIndex implements SearchIndex {
    public DefaultSearchIndex(Map configuration) {

    }

    @Override
    public void addElement(Graph graph, Element element) {

    }

    @Override
    public void removeElement(Graph graph, Element element) {

    }

    @Override
    public void addElements(Graph graph, Iterable<Element> elements) {
        for (Element element : elements) {
            addElement(graph, element);
        }
    }

    @Override
    public GraphQuery queryGraph(Graph graph, String queryString, Authorizations authorizations) {
        return new DefaultGraphQuery(graph, queryString, authorizations);
    }

    @Override
    public VertexQuery queryVertex(Graph graph, Vertex vertex, String queryString, Authorizations authorizations) {
        return new DefaultVertexQuery(graph, vertex, queryString, authorizations);
    }

    @Override
    public void flush() {

    }

    @Override
    public void shutdown() {

    }
}
