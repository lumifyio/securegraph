package com.altamiracorp.securegraph.inmemory;

import com.altamiracorp.securegraph.*;
import org.json.JSONObject;

public class InMemoryEdge extends InMemoryElement implements Edge {
    private final Object outVertexId;
    private final Object inVertexId;
    private final String label;

    protected InMemoryEdge(Graph graph, Object edgeId, Object outVertexId, Object inVertexId, String label, Visibility visibility, Property[] properties) {
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
    public JSONObject toJson() {
        JSONObject json = super.toJson();
        json.put("outVertexId", InMemoryGraph.objectToJsonString(this.outVertexId));
        json.put("inVertexId", InMemoryGraph.objectToJsonString(this.inVertexId));
        json.put("label", this.label);
        return json;
    }

    public static InMemoryEdge fromJson(InMemoryGraph graph, Object id, JSONObject jsonObject) {
        Visibility visibility = InMemoryElement.fromJsonVisibility(jsonObject);
        Property[] properties = InMemoryElement.fromJsonProperties(jsonObject);
        Object outVertexId = InMemoryGraph.jsonStringToObject(jsonObject.getString("outVertexId"));
        Object inVertexId = InMemoryGraph.jsonStringToObject(jsonObject.getString("inVertexId"));
        String label = jsonObject.getString("label");
        return new InMemoryEdge(graph, id, outVertexId, inVertexId, label, visibility, properties);
    }
}
