package com.altamiracorp.securegraph.query;

import com.altamiracorp.securegraph.Direction;
import com.altamiracorp.securegraph.Edge;

public interface VertexQuery extends Query {
    Iterable<Edge> edges(Direction direction);
}
