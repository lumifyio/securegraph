package org.securegraph.blueprints;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import org.securegraph.Authorizations;

public class SecureGraphBlueprintsEdge extends SecureGraphBlueprintsElement implements Edge {
    protected SecureGraphBlueprintsEdge(SecureGraphBlueprintsGraph graph, org.securegraph.Edge edge, Authorizations authorizations) {
        super(graph, edge, authorizations);
    }

    public static Edge create(SecureGraphBlueprintsGraph graph, org.securegraph.Edge edge, Authorizations authorizations) {
        if (edge == null) {
            return null;
        }
        return new SecureGraphBlueprintsEdge(graph, edge, authorizations);
    }

    @Override
    public Vertex getVertex(Direction direction) throws IllegalArgumentException {
        org.securegraph.Direction sgDirection = SecureGraphBlueprintsConvert.toSecureGraph(direction);
        Authorizations authorizations = getGraph().getAuthorizationsProvider().getAuthorizations();
        return SecureGraphBlueprintsVertex.create(getGraph(), getSecureGraphElement().getVertex(sgDirection, authorizations), authorizations);
    }

    @Override
    public String getLabel() {
        return getSecureGraphElement().getLabel();
    }

    @Override
    public void remove() {
        getGraph().removeEdge(this);
    }

    @Override
    public org.securegraph.Edge getSecureGraphElement() {
        return (org.securegraph.Edge) super.getSecureGraphElement();
    }

    @Override
    public void setProperty(String propertyName, Object value) {
        if ("label".equals(propertyName)) {
            throw new IllegalArgumentException("Property Name cannot be \"label\"");
        }
        super.setProperty(propertyName, value);
    }
}
