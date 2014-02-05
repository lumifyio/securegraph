package com.altamiracorp.securegraph.path;

import com.altamiracorp.securegraph.Authorizations;
import com.altamiracorp.securegraph.GraphBase;
import com.altamiracorp.securegraph.Path;
import com.altamiracorp.securegraph.Vertex;

public interface PathFindingAlgorithm {
    Iterable<Path> findPaths(GraphBase graphBase, Vertex sourceVertex, Vertex destVertex, int hops, Authorizations authorizations);
}
