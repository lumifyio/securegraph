package org.securegraph.inmemory;

import org.securegraph.*;
import org.securegraph.mutation.ExistingElementMutation;
import org.securegraph.mutation.ExistingElementMutationImpl;

public class InMemoryEdge extends InMemoryElement<Edge> implements Edge {
    private final String outVertexId;
    private final String inVertexId;
    private final String label;

    protected InMemoryEdge(Graph graph, String edgeId, String outVertexId, String inVertexId, String label, Visibility visibility, Iterable<Property> properties, Authorizations authorizations) {
        super(graph, edgeId, visibility, properties, authorizations);
        this.outVertexId = outVertexId;
        this.inVertexId = inVertexId;
        this.label = label;
    }

    @Override
    public String getLabel() {
        return this.label;
    }

    @Override
    public String getVertexId(Direction direction) {
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
    public String getOtherVertexId(String myVertexId) {
        if (inVertexId.equals(myVertexId)) {
            return outVertexId;
        } else if (outVertexId.equals(myVertexId)) {
            return inVertexId;
        }
        throw new SecureGraphException("myVertexId does not appear on either the in or the out.");
    }

    @Override
    public Vertex getOtherVertex(String myVertexId, Authorizations authorizations) {
        return getGraph().getVertex(getOtherVertexId(myVertexId), authorizations);
    }

    @Override
    public ExistingElementMutation<Edge> prepareMutation() {
        return new ExistingElementMutationImpl<Edge>(this) {
            @Override
            public Edge save(Authorizations authorizations) {
                saveExistingElementMutation(this, authorizations);
                return getElement();
            }
        };
    }
}
