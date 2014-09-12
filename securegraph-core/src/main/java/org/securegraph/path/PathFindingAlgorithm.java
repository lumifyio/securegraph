package org.securegraph.path;

import org.securegraph.Authorizations;
import org.securegraph.Graph;
import org.securegraph.Path;
import org.securegraph.Vertex;

public interface PathFindingAlgorithm {
    Iterable<Path> findPaths(Graph graph, Vertex sourceVertex, Vertex destVertex, int hops, Authorizations authorizations);
}
