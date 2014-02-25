package com.altamiracorp.securegraph.path;

import com.altamiracorp.securegraph.*;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.altamiracorp.securegraph.util.IterableUtils.toSet;

// TODO this algorithm could probably be better optimized to be iterable instead
public class RecursivePathFindingAlgorithm implements PathFindingAlgorithm {
    @Override
    public Iterable<Path> findPaths(GraphBase graphBase, final Vertex sourceVertex, Vertex destVertex, int hops, final Authorizations authorizations) {
        Set<Object> seenVertices = new HashSet<Object>();
        seenVertices.add(sourceVertex.getId());

        Path startPath = new Path(sourceVertex.getId());

        List<Path> foundPaths = new ArrayList<Path>();
        if (hops == 2) {
            findPathsSetIntersection(foundPaths, sourceVertex, destVertex, authorizations);
        } else {
            findPathsRecursive(foundPaths, sourceVertex, destVertex, hops, authorizations, seenVertices, startPath);
        }
        return foundPaths;
    }

    private void findPathsSetIntersection(List<Path> foundPaths, Vertex sourceVertex, Vertex destVertex, Authorizations authorizations) {
        Object sourceVertexId = sourceVertex.getId();
        Object destVertexId = destVertex.getId();
        Set<Object> sourceVertexConnectedVertexIds = toSet(sourceVertex.getVertexIds(Direction.BOTH, authorizations));
        Set<Object> destVertexConnectedVertexIds = toSet(destVertex.getVertexIds(Direction.BOTH, authorizations));

        sourceVertexConnectedVertexIds.retainAll(destVertexConnectedVertexIds);

        for (Object connectedVertexId : sourceVertexConnectedVertexIds) {
            foundPaths.add(new Path(sourceVertexId, connectedVertexId, destVertexId));
        }
    }

    private static void findPathsRecursive(List<Path> foundPaths, final Vertex sourceVertex, Vertex destVertex, int hops, final Authorizations authorizations, Set<Object> seenVertices, Path currentPath) {
        seenVertices.add(sourceVertex.getId());
        if (sourceVertex.getId().equals(destVertex.getId())) {
            foundPaths.add(currentPath);
        } else if (hops > 0) {
            for (Vertex child : sourceVertex.getVertices(Direction.BOTH, authorizations)) {
                if (!seenVertices.contains(child.getId())) {
                    findPathsRecursive(foundPaths, child, destVertex, hops - 1, authorizations, seenVertices, new Path(currentPath, child.getId()));
                }
            }
        }
        seenVertices.remove(sourceVertex.getId());
    }
}
