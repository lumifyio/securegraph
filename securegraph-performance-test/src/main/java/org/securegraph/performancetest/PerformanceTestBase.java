package org.securegraph.performancetest;

import org.securegraph.*;
import org.securegraph.util.MapUtils;

import java.io.*;
import java.util.Properties;

public abstract class PerformanceTestBase {
    public static final String CATEGORY_ID_PREFIX = "CATEGORY_";
    public static final Object MOVIE_ID_PREFIX = "MOVIE_";

    protected Graph createGraph() {
        try {
            Properties config = new Properties();
            InputStream in = new FileInputStream("config.properties");
            try {
                config.load(in);
            } finally {
                in.close();
            }
            return new GraphFactory().createGraph(MapUtils.getAllWithPrefix(config, "graph"));
        } catch (Exception ex) {
            throw new RuntimeException("Could not create graph", ex);
        }
    }

    protected void createVertices(Graph graph, int number, Authorizations authorizations) {
        Visibility visibility = new Visibility("");
        InputStream in = getClass().getResourceAsStream("/imdb.csv");
        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(in));
            for (int i = 0; i < number; i++) {
                String line = reader.readLine();
                if (line == null) {
                    throw new RuntimeException("Not enough lines in file. Needed " + number + " found " + i);
                }
                String[] parts = line.split("\t");
                String title = parts[0];
                int year = Integer.parseInt(parts[1]);
                double rating = Double.parseDouble(parts[2]);
                String[] categoriesArray;
                if (parts.length < 5) {
                    categoriesArray = new String[0];
                } else {
                    categoriesArray = parts[4].split(",");
                }

                Vertex movieVertex = graph.prepareVertex(MOVIE_ID_PREFIX + title, visibility)
                        .setProperty("title", title, visibility)
                        .setProperty("year", year, visibility)
                        .setProperty("rating", rating, visibility)
                        .save(authorizations);
                for (String category : categoriesArray) {
                    Vertex categoryVertex = graph.prepareVertex(CATEGORY_ID_PREFIX + category, visibility)
                            .setProperty("title", category, visibility)
                            .save(authorizations);
                    graph.addEdge(categoryVertex.getId() + "->" + movieVertex.getId(), categoryVertex, movieVertex, "hasMovie", visibility, authorizations);
                }
            }
            graph.flush();
        } catch (Exception e) {
            throw new RuntimeException("Could not create vertices", e);
        } finally {
            try {
                in.close();
            } catch (IOException e) {
                // do nothing
            }
        }
    }

    public static <T> int count(Iterable<T> iterable) {
        int count = 0;
        for (T ignore : iterable) {
            count++;
        }
        return count;
    }

    protected void time(String message, Runnable task) {
        long start = System.nanoTime();
        task.run();
        long elapsedTime = System.nanoTime() - start;
        System.out.println(message + ": " + (elapsedTime / 1000 / 1000) + "ms");
        System.out.flush();
    }
}
