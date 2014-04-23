package org.securegraph.util;

import org.securegraph.Authorizations;
import org.securegraph.Direction;
import org.securegraph.Vertex;

import java.util.Iterator;

public class VerticesToEdgeIdsIterable implements Iterable<Object> {
    private final Iterable<Vertex> vertices;
    private final Authorizations authorizations;

    public VerticesToEdgeIdsIterable(Iterable<Vertex> vertices, Authorizations authorizations) {
        this.vertices = vertices;
        this.authorizations = authorizations;
    }

    @Override
    public Iterator<Object> iterator() {
        return new SelectManyIterable<Vertex, Object>(this.vertices) {
            @Override
            public Iterable<Object> getIterable(Vertex vertex) {
                return vertex.getEdgeIds(Direction.BOTH, authorizations);
            }
        }.iterator();
    }
}
