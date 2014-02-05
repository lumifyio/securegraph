package com.altamiracorp.securegraph.path;

import com.altamiracorp.securegraph.*;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

// TODO this algorithm could probably be better optimized to be iterable instead
public class RecursivePathFindingAlgorithm implements PathFindingAlgorithm {
    @Override
    public Iterable<Path> findPaths(GraphBase graphBase, final Vertex sourceVertex, Vertex destVertex, int hops, final Authorizations authorizations) {
        Set<Object> seenVertices = new HashSet<Object>();
        seenVertices.add(sourceVertex.getId());

        Path startPath = new Path(sourceVertex.getId());

        List<Path> foundPaths = new ArrayList<Path>();
        findPaths(foundPaths, sourceVertex, destVertex, hops, authorizations, seenVertices, startPath);
        return foundPaths;
    }

    private static void findPaths(List<Path> foundPaths, final Vertex sourceVertex, Vertex destVertex, int hops, final Authorizations authorizations, Set<Object> seenVertices, Path currentPath) {
        for (Vertex child : sourceVertex.getVertices(Direction.BOTH, authorizations)) {
            if (seenVertices.contains(child.getId())) {
                continue;
            }

            if (child.getId().equals(destVertex.getId())) {
                foundPaths.add(new Path(currentPath, child.getId()));
                continue;
            }

            seenVertices.add(child.getId());
            if (hops > 0) {
                findPaths(foundPaths, child, destVertex, hops - 1, authorizations, seenVertices, new Path(currentPath, child.getId()));
            }
        }
    }
}
