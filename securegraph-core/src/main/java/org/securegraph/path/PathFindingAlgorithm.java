package org.securegraph.path;

import org.securegraph.Authorizations;
import org.securegraph.GraphBase;
import org.securegraph.Path;
import org.securegraph.Vertex;

public interface PathFindingAlgorithm {
    Iterable<Path> findPaths(GraphBase graphBase, Vertex sourceVertex, Vertex destVertex, int hops, Authorizations authorizations);
}
