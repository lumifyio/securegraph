package com.altamiracorp.securegraph.path;

import com.altamiracorp.securegraph.Authorizations;
import com.altamiracorp.securegraph.GraphBase;
import com.altamiracorp.securegraph.Vertex;

import java.util.List;

public interface PathFindingAlgorithm {
    Iterable<List<Object>> findPaths(GraphBase graphBase, Vertex sourceVertex, Vertex destVertex, int hops, Authorizations authorizations);
}
