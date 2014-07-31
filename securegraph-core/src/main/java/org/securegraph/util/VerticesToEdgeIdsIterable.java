package org.securegraph.util;

import org.securegraph.Authorizations;
import org.securegraph.Direction;
import org.securegraph.Vertex;

import java.util.Iterator;

public class VerticesToEdgeIdsIterable implements Iterable<String> {
    private final Iterable<Vertex> vertices;
    private final Authorizations authorizations;

    public VerticesToEdgeIdsIterable(Iterable<Vertex> vertices, Authorizations authorizations) {
        this.vertices = vertices;
        this.authorizations = authorizations;
    }

    @Override
    public Iterator<String> iterator() {
        return new SelectManyIterable<Vertex, String>(this.vertices) {
            @Override
            public Iterable<String> getIterable(Vertex vertex) {
                return vertex.getEdgeIds(Direction.BOTH, authorizations);
            }
        }.iterator();
    }
}
