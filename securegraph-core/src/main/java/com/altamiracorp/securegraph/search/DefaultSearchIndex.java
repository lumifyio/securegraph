package com.altamiracorp.securegraph.search;

import com.altamiracorp.securegraph.Authorizations;
import com.altamiracorp.securegraph.Element;
import com.altamiracorp.securegraph.Graph;
import com.altamiracorp.securegraph.Vertex;
import com.altamiracorp.securegraph.query.DefaultGraphQuery;
import com.altamiracorp.securegraph.query.DefaultVertexQuery;
import com.altamiracorp.securegraph.query.GraphQuery;
import com.altamiracorp.securegraph.query.VertexQuery;

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
    public GraphQuery queryGraph(Graph graph, String queryString, Authorizations authorizations) {
        return new DefaultGraphQuery(graph, queryString, authorizations);
    }

    @Override
    public VertexQuery queryVertex(Graph graph, Vertex vertex, String queryString, Authorizations authorizations) {
        return new DefaultVertexQuery(graph, vertex, queryString, authorizations);
    }
}
