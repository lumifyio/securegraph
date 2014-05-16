package org.securegraph.query;

import org.securegraph.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class DefaultGraphQuery extends GraphQueryBase {
    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultGraphQuery.class);

    public DefaultGraphQuery(Graph graph, String queryString, Map<String, PropertyDefinition> propertyDefinitions, Authorizations authorizations) {
        super(graph, queryString, propertyDefinitions, authorizations);
    }

    @Override
    public Iterable<Vertex> vertices() {
        LOGGER.warn("scanning all vertices! create your own GraphQuery.");
        return new DefaultGraphQueryIterable<Vertex>(getParameters(), this.<Vertex>getIterableFromElementType(ElementType.VERTEX), true);
    }

    @Override
    public Iterable<Edge> edges() {
        LOGGER.warn("scanning all edges! create your own GraphQuery.");
        return new DefaultGraphQueryIterable<Edge>(getParameters(), this.<Edge>getIterableFromElementType(ElementType.EDGE), true);
    }

    @SuppressWarnings("unchecked")
    private <T extends Element> Iterable<T> getIterableFromElementType(ElementType elementType) throws SecureGraphException {
        switch (elementType) {
            case VERTEX:
                return (Iterable<T>) getGraph().getVertices(getParameters().getAuthorizations());
            case EDGE:
                return (Iterable<T>) getGraph().getEdges(getParameters().getAuthorizations());
            default:
                throw new SecureGraphException("Unexpected element type: " + elementType);
        }
    }
}
