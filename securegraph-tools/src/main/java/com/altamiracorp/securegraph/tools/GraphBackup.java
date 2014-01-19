package com.altamiracorp.securegraph.tools;

import com.altamiracorp.securegraph.*;
import com.altamiracorp.securegraph.util.JavaSerializableUtils;
import org.apache.commons.codec.binary.Base64;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;

public class GraphBackup {
    public void save(Graph graph, OutputStream out, Authorizations authorizations) throws IOException {
        saveVertices(graph.getVertices(authorizations), out);
        saveEdges(graph.getEdges(authorizations), out);
    }

    private void saveVertices(Iterable<Vertex> vertices, OutputStream out) throws IOException {
        for (Vertex vertex : vertices) {
            JSONObject json = vertexToJson(vertex);
            out.write('V');
            out.write(json.toString().getBytes());
            out.write('\n');
        }
    }

    private void saveEdges(Iterable<Edge> edges, OutputStream out) throws IOException {
        for (Edge edge : edges) {
            JSONObject json = edgeToJson(edge);
            out.write('E');
            out.write(json.toString().getBytes());
            out.write('\n');
        }
    }

    private JSONObject vertexToJson(Vertex vertex) {
        return elementToJson(vertex);
    }

    private JSONObject edgeToJson(Edge edge) {
        JSONObject json = elementToJson(edge);
        json.put("outVertexId", objectToJsonString(edge.getVertexId(Direction.OUT)));
        json.put("inVertexId", objectToJsonString(edge.getVertexId(Direction.IN)));
        json.put("label", edge.getLabel());
        return json;
    }

    private JSONObject elementToJson(Element element) {
        JSONObject json = new JSONObject();
        json.put("id", objectToJsonString(element.getId()));
        json.put("visibility", element.getVisibility().getVisibilityString());
        json.put("properties", propertiesToJson(element.getProperties()));
        return json;
    }

    private JSONArray propertiesToJson(Iterable<Property> properties) {
        JSONArray json = new JSONArray();
        for (Property property : properties) {
            json.put(propertyToJson(property));
        }
        return json;
    }

    private JSONObject propertyToJson(Property property) {
        JSONObject json = new JSONObject();
        json.put("id", objectToJsonString(property.getId()));
        json.put("name", property.getName());
        json.put("visibility", property.getVisibility().getVisibilityString());
        json.put("value", objectToJsonString(property.getValue()));
        Map<String, Object> metadata = property.getMetadata();
        if (metadata != null) {
            json.put("metadata", metadataToJson(metadata));
        }
        return json;
    }

    private JSONObject metadataToJson(Map<String, Object> metadata) {
        JSONObject json = new JSONObject();
        for (Map.Entry<String, Object> m : metadata.entrySet()) {
            json.put(m.getKey(), objectToJsonString(m.getValue()));
        }
        return json;
    }

    private String objectToJsonString(Object value) {
        if (value instanceof String) {
            return (String) value;
        }
        return "base64/java:" + Base64.encodeBase64String(JavaSerializableUtils.objectToBytes(value));
    }
}
