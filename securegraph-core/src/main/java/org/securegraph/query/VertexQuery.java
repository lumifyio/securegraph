package org.securegraph.query;

import org.securegraph.Direction;
import org.securegraph.Edge;

public interface VertexQuery extends Query {
    Iterable<Edge> edges(Direction direction);

    Iterable<Edge> edges(Direction direction, String label);
}
