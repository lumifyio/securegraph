package org.securegraph.path;

import org.securegraph.*;

public interface PathFindingAlgorithm {
    Iterable<Path> findPaths(Graph graph, Vertex sourceVertex, Vertex destVertex, int hops, ProgressCallback progressCallback, Authorizations authorizations);
}
