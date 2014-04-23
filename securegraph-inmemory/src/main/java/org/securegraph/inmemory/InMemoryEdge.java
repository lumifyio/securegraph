package org.securegraph.inmemory;

import org.securegraph.*;
import org.securegraph.mutation.ExistingElementMutation;
import org.securegraph.mutation.ExistingElementMutationImpl;

public class InMemoryEdge extends InMemoryElement<Edge> implements Edge {
    private final Object outVertexId;
    private final Object inVertexId;
    private final String label;

    protected InMemoryEdge(Graph graph, Object edgeId, Object outVertexId, Object inVertexId, String label, Visibility visibility, Iterable<Property> properties) {
        super(graph, edgeId, visibility, properties);
        this.outVertexId = outVertexId;
        this.inVertexId = inVertexId;
        this.label = label;
    }

    @Override
    public String getLabel() {
        return this.label;
    }

    @Override
    public Object getVertexId(Direction direction) {
        switch (direction) {
            case IN:
                return inVertexId;
            case OUT:
                return outVertexId;
            default:
                throw new IllegalArgumentException("Unexpected direction: " + direction);
        }
    }

    @Override
    public Vertex getVertex(Direction direction, Authorizations authorizations) {
        return getGraph().getVertex(getVertexId(direction), authorizations);
    }

    @Override
    public Object getOtherVertexId(Object myVertexId) {
        if (inVertexId.equals(myVertexId)) {
            return outVertexId;
        } else if (outVertexId.equals(myVertexId)) {
            return inVertexId;
        }
        throw new SecureGraphException("myVertexId does not appear on either the in or the out.");
    }

    @Override
    public Vertex getOtherVertex(Object myVertexId, Authorizations authorizations) {
        return getGraph().getVertex(getOtherVertexId(myVertexId), authorizations);
    }

    @Override
    public ExistingElementMutation<Edge> prepareMutation() {
        return new ExistingElementMutationImpl<Edge>(this) {
            @Override
            public Edge save() {
                saveExistingElementMutation(this);
                return getElement();
            }
        };
    }
}
