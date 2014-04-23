package org.securegraph;

import org.securegraph.util.ArrayIterable;

import java.util.Arrays;
import java.util.Iterator;

public class Path implements Iterable<Object> {
    private final Object[] vertexIds;

    public Path(Object... vertexIds) {
        this.vertexIds = vertexIds;
    }

    public Path(Path path, Object vertexId) {
        this.vertexIds = Arrays.copyOf(path.vertexIds, path.vertexIds.length + 1);
        this.vertexIds[this.vertexIds.length - 1] = vertexId;
    }

    public int length() {
        return this.vertexIds.length;
    }

    public Object get(int i) {
        return this.vertexIds[i];
    }

    @Override
    public Iterator<Object> iterator() {
        return new ArrayIterable<Object>(this.vertexIds).iterator();
    }

    @Override
    public String toString() {
        return Arrays.toString(vertexIds);
    }
}
