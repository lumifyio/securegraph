package com.altamiracorp.securegraph.inmemory.util;

import com.altamiracorp.securegraph.Edge;
import com.altamiracorp.securegraph.util.ConvertingIterable;

public class EdgeToEdgeIdIterable extends ConvertingIterable<Edge, Object> {
    public EdgeToEdgeIdIterable(Iterable<Edge> edges) {
        super(edges);
    }

    @Override
    protected Object convert(Edge edge) {
        return edge.getId();
    }
}
