package org.securegraph.mutation;

import org.securegraph.Edge;

public interface EdgeMutation extends ElementMutation<Edge> {
    EdgeMutation alterEdgeLabel(String newEdgeLabel);

    String getNewEdgeLabel();
}
