package org.securegraph.cli.model;

import org.securegraph.Direction;
import org.securegraph.Edge;
import org.securegraph.Property;
import org.securegraph.cli.SecuregraphScript;

import java.io.PrintWriter;
import java.io.StringWriter;

public class LazyEdge extends ModelBase {
    private final String edgeId;

    public LazyEdge(String edgeId) {
        this.edgeId = edgeId;
    }

    @Override
    public String toString() {
        Edge e = getE();
        if (e == null) {
            return null;
        }

        return toString(e);
    }

    public static String toString(Edge e) {
        StringWriter out = new StringWriter();
        PrintWriter writer = new PrintWriter(out);
        writer.println("@|bold " + e.getId() + "|@");
        writer.println("  @|bold visibility:|@ " + e.getVisibility());
        writer.println("  @|bold label:|@ " + e.getLabel());

        writer.println("  @|bold properties:|@");
        SecuregraphScript.getContextProperties().clear();
        int propIndex = 0;
        for (Property prop : e.getProperties()) {
            String propertyIndexString = "p" + propIndex;
            String valueString = SecuregraphScript.valueToString(prop.getValue(), false);
            writer.println("    @|bold " + propertyIndexString + ":|@ " + prop.getName() + ":" + prop.getKey() + "[" + prop.getVisibility().getVisibilityString() + "] = " + valueString);
            LazyProperty lazyProperty = new LazyEdgeProperty(e.getId(), prop.getKey(), prop.getName(), prop.getVisibility());
            SecuregraphScript.getContextProperties().put(propertyIndexString, lazyProperty);
            propIndex++;
        }

        SecuregraphScript.getContextVertices().clear();
        int vertexIndex = 0;

        writer.println("  @|bold out vertex:|@");
        String vertexIndexString = "v" + vertexIndex;
        writer.println("    @|bold " + vertexIndexString + ":|@ " + e.getVertexId(Direction.OUT));
        LazyVertex lazyVertex = new LazyVertex(e.getVertexId(Direction.OUT));
        SecuregraphScript.getContextVertices().put(vertexIndexString, lazyVertex);
        vertexIndex++;

        writer.println("  @|bold in vertex:|@");
        vertexIndexString = "v" + vertexIndex;
        writer.println("    @|bold " + vertexIndexString + ":|@ " + e.getVertexId(Direction.IN));
        lazyVertex = new LazyVertex(e.getVertexId(Direction.IN));
        SecuregraphScript.getContextVertices().put(vertexIndexString, lazyVertex);
        vertexIndex++;

        return out.toString();
    }

    private Edge getE() {
        return getGraph().getEdge(getId(), getAuthorizations());
    }

    public String getId() {
        return edgeId;
    }
}
