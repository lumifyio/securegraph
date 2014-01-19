package com.altamiracorp.securegraph.tools;

import com.altamiracorp.securegraph.*;
import com.altamiracorp.securegraph.util.JavaSerializableUtils;
import com.beust.jcommander.Parameter;
import org.apache.commons.codec.binary.Base64;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

public class GraphRestore extends GraphToolBase {
    @Parameter(names = {"--in", "-i"}, description = "Input filename")
    private String inputFileName = null;

    public static void main(String[] args) throws Exception {
        GraphRestore graphRestore = new GraphRestore();
        graphRestore.run(args);
    }

    protected void run(String[] args) throws Exception {
        super.run(args);

        InputStream in = createInputStream();
        try {
            restore(getGraph(), in, getAuthorizations());
        } finally {
            in.close();
        }
    }

    private InputStream createInputStream() throws FileNotFoundException {
        if (inputFileName == null) {
            return System.in;
        }
        return new FileInputStream(inputFileName);
    }

    public void restore(Graph graph, InputStream in, Authorizations authorizations) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(in));
        String line;
        char lastType = 'V';
        while ((line = reader.readLine()) != null) {
            char type = line.charAt(0);
            JSONObject json = new JSONObject(line.substring(1));
            switch (type) {
                case 'V':
                    restoreVertex(graph, json);
                    break;
                case 'E':
                    if (type != lastType) {
                        graph.flush();
                    }
                    restoreEdge(graph, json, authorizations);
                    break;
                default:
                    throw new RuntimeException("Unexpected line: " + line);
            }
            lastType = type;
        }
    }

    private void restoreVertex(Graph graph, JSONObject json) {
        Visibility visibility = jsonToVisibility(json);
        Object vertexId = jsonStringToObject(json.getString("id"));
        VertexBuilder v = graph.prepareVertex(vertexId, visibility);
        jsonToProperties(json, v);
        v.save();
    }

    private void restoreEdge(Graph graph, JSONObject json, Authorizations authorizations) {
        Visibility visibility = jsonToVisibility(json);
        Object edgeId = jsonStringToObject(json.getString("id"));
        Object outVertexId = jsonStringToObject(json.getString("outVertexId"));
        Object inVertexId = jsonStringToObject(json.getString("inVertexId"));
        String label = json.getString("label");
        Vertex outVertex = graph.getVertex(outVertexId, authorizations);
        Vertex inVertex = graph.getVertex(inVertexId, authorizations);
        EdgeBuilder e = graph.prepareEdge(edgeId, outVertex, inVertex, label, visibility);
        jsonToProperties(json, e);
        e.save();
    }

    protected Visibility jsonToVisibility(JSONObject jsonObject) {
        return new Visibility(jsonObject.getString("visibility"));
    }

    protected void jsonToProperties(JSONObject jsonObject, ElementBuilder e) {
        JSONArray propertiesJson = jsonObject.getJSONArray("properties");
        for (int i = 0; i < propertiesJson.length(); i++) {
            JSONObject propertyJson = propertiesJson.getJSONObject(i);
            jsonToProperty(propertyJson, e);
        }
    }

    private void jsonToProperty(JSONObject propertyJson, ElementBuilder e) {
        Object id = jsonStringToObject(propertyJson.getString("id"));
        String name = propertyJson.getString("name");
        Object value = jsonStringToObject(propertyJson.getString("value"));
        Map<String, Object> metadata = jsonToPropertyMetadata(propertyJson.optJSONObject("metadata"));
        Visibility visibility = new Visibility(propertyJson.getString("visibility"));
        e.addPropertyValue(id, name, value, metadata, visibility);
    }

    private Map<String, Object> jsonToPropertyMetadata(JSONObject metadataJson) {
        Map<String, Object> metadata = new HashMap<String, Object>();
        if (metadataJson == null) {
            return metadata;
        }
        for (Object key : metadataJson.keySet()) {
            String keyString = (String) key;
            Object val = jsonStringToObject(metadataJson.getString(keyString));
            metadata.put(keyString, val);
        }
        return metadata;
    }

    private Object jsonStringToObject(String str) {
        if (str.startsWith("base64/java:")) {
            return JavaSerializableUtils.bytesToObject(Base64.decodeBase64(str));
        } else {
            return str;
        }
    }
}
