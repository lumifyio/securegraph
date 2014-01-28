package com.altamiracorp.securegraph.test.performance;

import com.altamiracorp.securegraph.Authorizations;
import com.altamiracorp.securegraph.Graph;
import com.altamiracorp.securegraph.accumulo.AccumuloAuthorizations;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
@Category(PerformanceTest.class)
public class SearchAndRetrievePerformanceTest extends PerformanceTestBase {
    public static void main(String[] args) throws InterruptedException {
        new SearchAndRetrievePerformanceTest().testSearch();
    }

    @Test
    public void testSearch() {
        time("total", new Runnable() {
            @Override
            public void run() {
                final Authorizations authorizations = new AccumuloAuthorizations();
                final Graph graph = createGraph();
                time("insert", new Runnable() {
                    @Override
                    public void run() {
                        createVertices(graph, 3000, authorizations);
                    }
                });


                try {
                    Thread.sleep(10000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                time("query", new Runnable() {
                    @Override
                    public void run() {
                        count(graph.query(authorizations)
                                .has("title", "night")
                                .vertices());
                        count(graph.query(authorizations)
                                .has("title", "city")
                                .vertices());
                        count(graph.query(authorizations)
                                .has("title", "wild")
                                .vertices());
                    }
                });
            }
        });
    }
}
