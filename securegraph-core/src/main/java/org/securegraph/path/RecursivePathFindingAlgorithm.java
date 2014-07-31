package org.securegraph.path;

import org.securegraph.*;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.securegraph.util.IterableUtils.toSet;

// TODO this algorithm could probably be better optimized to be iterable instead
public class RecursivePathFindingAlgorithm implements PathFindingAlgorithm {
    @Override
    public Iterable<Path> findPaths(GraphBase graphBase, final Vertex sourceVertex, Vertex destVertex, int hops, final Authorizations authorizations) {
        Set<String> seenVertices = new HashSet<String>();
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
        String sourceVertexId = sourceVertex.getId();
        String destVertexId = destVertex.getId();
        Set<String> sourceVertexConnectedVertexIds = toSet(sourceVertex.getVertexIds(Direction.BOTH, authorizations));
        Set<String> destVertexConnectedVertexIds = toSet(destVertex.getVertexIds(Direction.BOTH, authorizations));

        sourceVertexConnectedVertexIds.retainAll(destVertexConnectedVertexIds);

        for (String connectedVertexId : sourceVertexConnectedVertexIds) {
            foundPaths.add(new Path(sourceVertexId, connectedVertexId, destVertexId));
        }
    }

    private static void findPathsRecursive(List<Path> foundPaths, final Vertex sourceVertex, Vertex destVertex, int hops, final Authorizations authorizations, Set<String> seenVertices, Path currentPath) {
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
