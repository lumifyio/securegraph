package org.securegraph.tools;

import org.securegraph.*;
import org.securegraph.property.StreamingPropertyValue;
import org.securegraph.util.JavaSerializableUtils;
import com.beust.jcommander.Parameter;
import org.apache.commons.codec.binary.Base64;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.*;
import java.util.Map;

public class GraphBackup extends GraphToolBase {
    @Parameter(names = {"--out", "-o"}, description = "Output filename")
    private String outputFileName = null;

    public static void main(String[] args) throws Exception {
        GraphBackup graphBackup = new GraphBackup();
        graphBackup.run(args);
    }

    protected void run(String[] args) throws Exception {
        super.run(args);

        OutputStream out = createOutputStream();
        try {
            save(getGraph(), out, getAuthorizations());
        } finally {
            out.close();
        }
    }

    private OutputStream createOutputStream() throws FileNotFoundException {
        if (outputFileName == null) {
            return System.out;
        }
        return new FileOutputStream(outputFileName);
    }

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
            saveStreamingPropertyValues(out, vertex);
        }
    }

    private void saveEdges(Iterable<Edge> edges, OutputStream out) throws IOException {
        for (Edge edge : edges) {
            JSONObject json = edgeToJson(edge);
            out.write('E');
            out.write(json.toString().getBytes());
            out.write('\n');
            saveStreamingPropertyValues(out, edge);
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
            if (property.getValue() instanceof StreamingPropertyValue) {
                continue;
            }
            json.put(propertyToJson(property));
        }
        return json;
    }

    private JSONObject propertyToJson(Property property) {
        JSONObject json = new JSONObject();
        json.put("key", property.getKey());
        json.put("name", property.getName());
        json.put("visibility", property.getVisibility().getVisibilityString());
        Object value = property.getValue();
        if (!(value instanceof StreamingPropertyValue)) {
            json.put("value", objectToJsonString(value));
        }
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

    private void saveStreamingPropertyValues(OutputStream out, Element element) throws IOException {
        for (Property property : element.getProperties()) {
            if (property.getValue() instanceof StreamingPropertyValue) {
                saveStreamingProperty(out, property);
            }
        }
    }

    private void saveStreamingProperty(OutputStream out, Property property) throws IOException {
        StreamingPropertyValue spv = (StreamingPropertyValue) property.getValue();
        JSONObject json = propertyToJson(property);
        json.put("valueType", spv.getValueType().getName());
        out.write('D');
        out.write(json.toString().getBytes());
        out.write('\n');
        InputStream in = spv.getInputStream();
        byte[] buffer = new byte[10 * 1024];
        int read;
        while ((read = in.read(buffer)) > 0) {
            out.write(Integer.toString(read).getBytes());
            out.write('\n');
            out.write(buffer, 0, read);
            out.write('\n');
        }
        out.write('0');
        out.write('\n');
    }

    private String objectToJsonString(Object value) {
        if (value instanceof String) {
            return (String) value;
        }
        return "base64/java:" + Base64.encodeBase64String(JavaSerializableUtils.objectToBytes(value));
    }
}
