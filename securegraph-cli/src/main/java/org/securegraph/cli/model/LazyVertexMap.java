package org.securegraph.cli.model;

import org.securegraph.Vertex;
import org.securegraph.cli.SecuregraphScript;

public class LazyVertexMap extends ModelBase {
    public LazyVertexMap(SecuregraphScript script) {
        super(script);
    }

    public LazyVertex get(String vertexId) {
        Vertex v = getGraph().getVertex(vertexId, getScript().getAuthorizations());
        if (v == null) {
            return null;
        }
        return new LazyVertex(getScript(), vertexId);
    }
}
