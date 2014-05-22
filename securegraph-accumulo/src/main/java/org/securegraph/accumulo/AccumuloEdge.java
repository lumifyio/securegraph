package org.securegraph.accumulo;

import org.apache.hadoop.io.Text;
import org.securegraph.*;
import org.securegraph.mutation.ExistingElementMutation;
import org.securegraph.mutation.ExistingElementMutationImpl;

public class AccumuloEdge extends AccumuloElement<Edge> implements Edge {
    public static final Text CF_SIGNAL = new Text("E");
    public static final Text CF_OUT_VERTEX = new Text("EOUT");
    public static final Text CF_IN_VERTEX = new Text("EIN");
    private final Object outVertexId;
    private final Object inVertexId;
    private final String label;

    public AccumuloEdge(Graph graph, Object id, Object outVertexId, Object inVertexId, String label, Visibility visibility, Iterable<Property> properties, Authorizations authorizations) {
        super(graph, id, visibility, properties, authorizations);
        this.outVertexId = outVertexId;
        this.inVertexId = inVertexId;
        this.label = label;
    }

    @Override
    public String getLabel() {
        return label;
    }

    @Override
    public Object getVertexId(Direction direction) {
        switch (direction) {
            case OUT:
                return outVertexId;
            case IN:
                return inVertexId;
            default:
                throw new IllegalArgumentException("Unexpected direction: " + direction);
        }
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
    public Vertex getVertex(Direction direction, Authorizations authorizations) {
        return getGraph().getVertex(getVertexId(direction), authorizations);
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
