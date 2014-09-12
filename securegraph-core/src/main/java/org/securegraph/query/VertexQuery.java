package org.securegraph.query;

import org.securegraph.Direction;
import org.securegraph.Edge;
import org.securegraph.FetchHint;

import java.util.EnumSet;

public interface VertexQuery extends Query {
    Iterable<Edge> edges(Direction direction);

    Iterable<Edge> edges(Direction direction, EnumSet<FetchHint> fetchHints);

    Iterable<Edge> edges(Direction direction, String label);

    Iterable<Edge> edges(Direction direction, String label, EnumSet<FetchHint> fetchHints);
}
