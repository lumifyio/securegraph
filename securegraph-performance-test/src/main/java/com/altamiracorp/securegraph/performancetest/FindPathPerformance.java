package com.altamiracorp.securegraph.performancetest;

import com.altamiracorp.securegraph.Authorizations;
import com.altamiracorp.securegraph.Graph;
import com.altamiracorp.securegraph.Vertex;
import com.altamiracorp.securegraph.accumulo.AccumuloAuthorizations;

public class FindPathPerformance extends PerformanceTestBase {
    public static final int NUMBER_OF_VERTICES_TO_CREATE = 3000;

    public static void main(String[] args) throws InterruptedException {
        new FindPathPerformance().testSearch();
    }

    public void testSearch() {
        time("total", new Runnable() {
            @Override
            public void run() {
                final Authorizations authorizations = new AccumuloAuthorizations();
                final Graph graph = createGraph();
                time("insert", new Runnable() {
                    @Override
                    public void run() {
                        createVertices(graph, NUMBER_OF_VERTICES_TO_CREATE, authorizations);
                    }
                });

                final Vertex starTrekVertex = graph.getVertex(MOVIE_ID_PREFIX + "Star Trek", authorizations);
                final Vertex contactVertex = graph.getVertex(MOVIE_ID_PREFIX + "Contact", authorizations);
                time("query 2 hops", new Runnable() {
                    @Override
                    public void run() {
                        int count = count(graph.findPaths(starTrekVertex, contactVertex, 2, authorizations));
                        System.out.println("Found " + count + " paths with 2 hops");
                    }
                });
                time("query 3 hops", new Runnable() {
                    @Override
                    public void run() {
                        int count = count(graph.findPaths(starTrekVertex, contactVertex, 3, authorizations));
                        System.out.println("Found " + count + " paths with 3 hops");
                    }
                });
            }
        });
    }
}
