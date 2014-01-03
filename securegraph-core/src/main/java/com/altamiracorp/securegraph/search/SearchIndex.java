package com.altamiracorp.securegraph.search;

import com.altamiracorp.securegraph.Authorizations;
import com.altamiracorp.securegraph.Element;
import com.altamiracorp.securegraph.Graph;
import com.altamiracorp.securegraph.Vertex;
import com.altamiracorp.securegraph.query.GraphQuery;
import com.altamiracorp.securegraph.query.VertexQuery;

public interface SearchIndex {
    void addElement(Graph graph, Element element);

    void removeElement(Graph graph, Element element);

    GraphQuery queryGraph(Graph graph, Authorizations authorizations);

    VertexQuery queryVertex(Graph graph, Vertex vertex, Authorizations authorizations);
}
