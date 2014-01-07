package com.altamiracorp.securegraph.path;

import com.altamiracorp.securegraph.Authorizations;
import com.altamiracorp.securegraph.Direction;
import com.altamiracorp.securegraph.GraphBase;
import com.altamiracorp.securegraph.Vertex;

import java.util.*;

// TODO this algorithm could probably be better optimized to be iterable instead
public class RecursivePathFindingAlgorithm implements PathFindingAlgorithm {
    @Override
    public Iterable<List<Object>> findPaths(GraphBase graphBase, final Vertex sourceVertex, Vertex destVertex, int hops, final Authorizations authorizations) {
        HashMap<Object, Vertex> seenVertices = new HashMap<Object, Vertex>();
        seenVertices.put(sourceVertex.getId(), sourceVertex);

        ArrayList<Object> currentPath = new ArrayList<Object>();
        currentPath.add(sourceVertex.getId());

        return findPaths(graphBase, sourceVertex, destVertex, hops, authorizations, seenVertices, currentPath);
    }

    public Collection<List<Object>> findPaths(GraphBase graphBase, final Vertex sourceVertex, Vertex destVertex, int hops, final Authorizations authorizations, Map<Object, Vertex> seenVertices, Collection<Object> currentPath) {
        List<List<Object>> paths = new ArrayList<List<Object>>();

        for (Vertex child : sourceVertex.getVertices(Direction.BOTH, authorizations)) {
            if (seenVertices.containsKey(child.getId())) {
                continue;
            }
            List<Object> path = createPath(currentPath, child);
            if (child.getId().equals(destVertex.getId())) {
                paths.add(path);
            } else {
                seenVertices.put(child.getId(), child);
                if (hops > 0) {
                    Collection<List<Object>> childPaths = findPaths(graphBase, child, destVertex, hops - 1, authorizations, seenVertices, path);
                    paths.addAll(childPaths);
                }
            }
        }

        return paths;
    }

    private List<Object> createPath(Collection<Object> currentPath, Vertex child) {
        List<Object> path = new ArrayList<Object>();
        path.addAll(currentPath);
        path.add(child.getId());
        return path;
    }
}
