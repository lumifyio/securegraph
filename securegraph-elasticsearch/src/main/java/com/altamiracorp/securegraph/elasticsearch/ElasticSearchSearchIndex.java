package com.altamiracorp.securegraph.elasticsearch;

import com.altamiracorp.securegraph.Authorizations;
import com.altamiracorp.securegraph.Element;
import com.altamiracorp.securegraph.Graph;
import com.altamiracorp.securegraph.Vertex;
import com.altamiracorp.securegraph.query.DefaultGraphQuery;
import com.altamiracorp.securegraph.query.DefaultVertexQuery;
import com.altamiracorp.securegraph.query.GraphQuery;
import com.altamiracorp.securegraph.query.VertexQuery;
import com.altamiracorp.securegraph.search.SearchIndex;

import java.util.Map;

public class ElasticSearchSearchIndex implements SearchIndex {
    public ElasticSearchSearchIndex(Map config) {

    }

    @Override
    public void addElement(Graph graph, Element element) {

    }

    @Override
    public void removeElement(Graph graph, Element element) {

    }

    @Override
    public GraphQuery queryGraph(Graph graph, Authorizations authorizations) {
        return new DefaultGraphQuery(graph, authorizations);
    }

    @Override
    public VertexQuery queryVertex(Graph graph, Vertex vertex, Authorizations authorizations) {
        return new DefaultVertexQuery(graph, vertex, authorizations);
    }
}
