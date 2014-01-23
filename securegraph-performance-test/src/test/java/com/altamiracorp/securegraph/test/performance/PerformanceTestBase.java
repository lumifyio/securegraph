package com.altamiracorp.securegraph.test.performance;

import com.altamiracorp.securegraph.Authorizations;
import com.altamiracorp.securegraph.Graph;
import com.altamiracorp.securegraph.GraphFactory;
import com.altamiracorp.securegraph.Visibility;
import com.altamiracorp.securegraph.util.MapUtils;

import java.io.*;
import java.util.Properties;

public abstract class PerformanceTestBase {
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
                graph.prepareVertex(visibility, authorizations)
                        .setProperty("title", title, visibility)
                        .save();
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
