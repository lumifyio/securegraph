package org.securegraph.cli.model;

import org.securegraph.Direction;
import org.securegraph.Edge;
import org.securegraph.Property;
import org.securegraph.Vertex;
import org.securegraph.cli.SecuregraphScript;

import java.io.PrintWriter;
import java.io.StringWriter;

public class LazyVertex extends ModelBase {
    private final String vertexId;

    public LazyVertex(SecuregraphScript script, String vertexId) {
        super(script);
        this.vertexId = vertexId;
    }

    @Override
    public String toString() {
        Vertex v = getV();
        if (v == null) {
            return null;
        }

        StringWriter out = new StringWriter();
        PrintWriter writer = new PrintWriter(out);
        writer.println("@|bold " + v.getId() + "|@");
        writer.println("  @|bold visibility:|@ " + v.getVisibility());

        writer.println("  @|bold properties:|@");
        getScript().getContextProperties().clear();
        int propIndex = 0;
        for (Property prop : v.getProperties()) {
            String propertyIndexString = "p" + propIndex;
            String valueString = valueToString(prop.getValue());
            writer.println("    @|bold " + propertyIndexString + ":|@ " + prop.getName() + "[" + prop.getVisibility().getVisibilityString() + "] = " + valueString);
            LazyProperty lazyProperty = new LazyVertexProperty(getScript(), this.vertexId, prop.getKey(), prop.getName(), prop.getVisibility());
            getScript().getContextProperties().put(propertyIndexString, lazyProperty);
            propIndex++;
        }

        getScript().getContextEdges().clear();
        int edgeIndex = 0;

        writer.println("  @|bold out edges:|@");
        for (Edge edge : v.getEdges(Direction.OUT, getAuthorizations())) {
            String edgeIndexString = "e" + edgeIndex;
            writer.println("    @|bold " + edgeIndexString + ":|@ " + edge.getLabel() + " -> " + edge.getOtherVertexId(vertexId));
            LazyEdge lazyEdge = new LazyEdge(getScript(), edge.getId());
            getScript().getContextEdges().put(edgeIndexString, lazyEdge);
            edgeIndex++;
        }

        writer.println("  @|bold in edges:|@");
        for (Edge edge : v.getEdges(Direction.IN, getAuthorizations())) {
            String edgeIndexString = "e" + edgeIndex;
            writer.println("    @|bold " + edgeIndexString + ":|@ " + edge.getLabel() + " -> " + edge.getOtherVertexId(vertexId));
            LazyEdge lazyEdge = new LazyEdge(getScript(), edge.getId());
            getScript().getContextEdges().put(edgeIndexString, lazyEdge);
            edgeIndex++;
        }

        return out.toString();
    }

    private Vertex getV() {
        return getGraph().getVertex(getId(), getAuthorizations());
    }

    public String getId() {
        return vertexId;
    }
}
