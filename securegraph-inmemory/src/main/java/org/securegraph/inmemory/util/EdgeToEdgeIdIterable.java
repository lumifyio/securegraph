package org.securegraph.inmemory.util;

import org.securegraph.Edge;
import org.securegraph.util.ConvertingIterable;

public class EdgeToEdgeIdIterable extends ConvertingIterable<Edge, String> {
    public EdgeToEdgeIdIterable(Iterable<Edge> edges) {
        super(edges);
    }

    @Override
    protected String convert(Edge edge) {
        return edge.getId();
    }
}
