package com.altamiracorp.securegraph.elasticsearch;

import com.altamiracorp.securegraph.*;
import com.altamiracorp.securegraph.query.DefaultGraphQuery;
import com.altamiracorp.securegraph.query.DefaultVertexQuery;
import com.altamiracorp.securegraph.query.GraphQuery;
import com.altamiracorp.securegraph.query.VertexQuery;
import com.altamiracorp.securegraph.search.SearchIndex;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import java.util.Map;

public class ElasticSearchSearchIndex implements SearchIndex {
    public static final String ES_LOCATIONS = "locations";
    private final TransportClient client;

    public ElasticSearchSearchIndex(Map config) {
        String esLocationsString = (String) config.get(ES_LOCATIONS);
        if (esLocationsString == null) {
            throw new SecureGraphException(ES_LOCATIONS + " is a required configuration parameter");
        }
        String[] esLocations = esLocationsString.split(",");

        client = new TransportClient();
        for (String esLocation : esLocations) {
            String[] locationSocket = esLocation.split(":");
            client.addTransportAddress(new InetSocketTransportAddress(locationSocket[0], Integer.parseInt(locationSocket[1])));
        }
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
