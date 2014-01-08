package com.altamiracorp.securegraph.inmemory;

import com.altamiracorp.securegraph.*;
import com.altamiracorp.securegraph.id.IdGenerator;
import com.altamiracorp.securegraph.search.SearchIndex;
import com.altamiracorp.securegraph.util.JavaSerializableUtils;
import com.altamiracorp.securegraph.util.LookAheadIterable;
import com.altamiracorp.securegraph.util.StreamUtils;
import org.apache.commons.codec.binary.Base64;
import org.json.JSONObject;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;

public class InMemoryGraph extends GraphBase {
    private final Map<Object, InMemoryVertex> vertices;
    private final Map<Object, InMemoryEdge> edges;

    public InMemoryGraph(GraphConfiguration configuration, IdGenerator idGenerator, SearchIndex searchIndex) {
        this(configuration, idGenerator, searchIndex, new HashMap<Object, InMemoryVertex>(), new HashMap<Object, InMemoryEdge>());
    }

    protected InMemoryGraph(GraphConfiguration configuration, IdGenerator idGenerator, SearchIndex searchIndex, Map<Object, InMemoryVertex> vertices, Map<Object, InMemoryEdge> edges) {
        super(configuration, idGenerator, searchIndex);
        this.vertices = vertices;
        this.edges = edges;
    }

    @Override
    public Vertex addVertex(Object vertexId, Visibility visibility, Property... properties) {
        if (vertexId == null) {
            vertexId = getIdGenerator().nextId();
        }

        InMemoryVertex vertex = new InMemoryVertex(this, vertexId, visibility, properties);
        vertices.put(vertexId, vertex);

        getSearchIndex().addElement(this, vertex);

        return vertex;
    }

    @Override
    public Iterable<Vertex> getVertices(final Authorizations authorizations) throws SecureGraphException {
        return new LookAheadIterable<InMemoryVertex, Vertex>() {
            @Override
            protected boolean isIncluded(InMemoryVertex src, Vertex edge) {
                return hasAccess(src.getVisibility(), authorizations);
            }

            @Override
            protected Vertex convert(InMemoryVertex vertex) {
                return filteredVertex(vertex, authorizations);
            }

            @Override
            protected Iterator<InMemoryVertex> createIterator() {
                return vertices.values().iterator();
            }
        };
    }

    @Override
    public void removeVertex(Vertex vertex, Authorizations authorizations) {
        if (!hasAccess(vertex.getVisibility(), authorizations)) {
            return;
        }
        this.vertices.remove(vertex.getId());
        getSearchIndex().removeElement(this, vertex);
    }

    @Override
    public Edge addEdge(Object edgeId, Vertex outVertex, Vertex inVertex, String label, Visibility visibility, Property... properties) {
        if (edgeId == null) {
            edgeId = getIdGenerator().nextId();
        }

        InMemoryEdge edge = new InMemoryEdge(this, edgeId, outVertex.getId(), inVertex.getId(), label, visibility, properties);
        edges.put(edgeId, edge);

        getSearchIndex().addElement(this, edge);

        return edge;
    }

    @Override
    public Iterable<Edge> getEdges(final Authorizations authorizations) {
        return new LookAheadIterable<InMemoryEdge, Edge>() {
            @Override
            protected boolean isIncluded(InMemoryEdge src, Edge edge) {
                return hasAccess(src.getVisibility(), authorizations);
            }

            @Override
            protected Edge convert(InMemoryEdge edge) {
                return filteredEdge(edge, authorizations);
            }

            @Override
            protected Iterator<InMemoryEdge> createIterator() {
                return edges.values().iterator();
            }
        };
    }

    @Override
    public void removeEdge(Edge edge, Authorizations authorizations) {
        if (!hasAccess(edge.getVisibility(), authorizations)) {
            return;
        }
        this.edges.remove(edge.getId());
        getSearchIndex().removeElement(this, edge);
    }

    @Override
    public void flush() {

    }

    @Override
    public Property createProperty(Object id, String name, Object value, Map<String, Object> metadata, Visibility visibility) {
        return new InMemoryProperty(id, name, value, metadata, visibility);
    }

    @Override
    public void shutdown() {

    }

    public Iterable<Edge> getEdgesFromVertex(final Object vertexId, final Authorizations authorizations) {
        return new LookAheadIterable<InMemoryEdge, Edge>() {
            @Override
            protected boolean isIncluded(InMemoryEdge src, Edge edge) {
                if (!src.getVertexId(Direction.IN).equals(vertexId) && !src.getVertexId(Direction.OUT).equals(vertexId)) {
                    return false;
                }
                return hasAccess(src.getVisibility(), authorizations);
            }

            @Override
            protected Edge convert(InMemoryEdge edge) {
                return filteredEdge(edge, authorizations);
            }

            @Override
            protected Iterator<InMemoryEdge> createIterator() {
                return edges.values().iterator();
            }
        };
    }

    private boolean hasAccess(Visibility visibility, Authorizations authorizations) {
        // TODO handle more complex accessibility. borrow code from Accumulo?
        for (String a : authorizations.getAuthorizations()) {
            if (visibility.getVisibilityString().equals(a)) {
                return true;
            }
        }
        return false;
    }

    public void saveProperties(Element element, Property[] properties) {
        if (element instanceof Vertex) {
            InMemoryVertex vertex = vertices.get(element.getId());
            vertex.setPropertiesInternal(properties);
        } else if (element instanceof Edge) {
            InMemoryEdge edge = edges.get(element.getId());
            edge.setPropertiesInternal(properties);
        } else {
            throw new IllegalArgumentException("Unexpected element type: " + element.getClass().getName());
        }
        getSearchIndex().addElement(this, element);
    }

    public void removeProperty(Element element, Property property) {
        if (element instanceof Vertex) {
            InMemoryVertex vertex = vertices.get(element.getId());
            vertex.removePropertyInternal(property.getId(), property.getName());
        } else if (element instanceof Edge) {
            InMemoryEdge edge = edges.get(element.getId());
            edge.removePropertyInternal(property.getId(), property.getName());
        } else {
            throw new IllegalArgumentException("Unexpected element type: " + element.getClass().getName());
        }
        getSearchIndex().removeElement(this, element);
    }

    private Edge filteredEdge(InMemoryEdge edge, Authorizations authorizations) {
        Object edgeId = edge.getId();
        Object outVertexId = edge.getVertexId(Direction.OUT);
        Object inVertexId = edge.getVertexId(Direction.IN);
        String label = edge.getLabel();
        Visibility visibility = edge.getVisibility();
        Property[] properties = filterProperties(edge.getProperties(), authorizations);
        return new InMemoryEdge(this, edgeId, outVertexId, inVertexId, label, visibility, properties);
    }

    private Vertex filteredVertex(InMemoryVertex vertex, Authorizations authorizations) {
        Object vertexId = vertex.getId();
        Visibility visibility = vertex.getVisibility();
        Property[] properties = filterProperties(vertex.getProperties(), authorizations);
        return new InMemoryVertex(this, vertexId, visibility, properties);
    }

    private Property[] filterProperties(Iterable<Property> properties, Authorizations authorizations) {
        List<Property> filteredProperties = new ArrayList<Property>();
        for (Property p : properties) {
            if (hasAccess(p.getVisibility(), authorizations)) {
                filteredProperties.add(p);
            }
        }
        return filteredProperties.toArray(new Property[filteredProperties.size()]);
    }

    public void save(OutputStream out) throws IOException {
        out.write(toJson().toString(2).getBytes());
    }

    public JSONObject toJson() {
        JSONObject json = new JSONObject();
        json.put("config", toJsonConfig());
        json.put("vertices", toJsonVertices());
        json.put("edges", toJsonEdges());
        return json;
    }

    private JSONObject toJsonConfig() {
        JSONObject json = new JSONObject();
        for (Object e : getConfiguration().getConfig().entrySet()) {
            Map.Entry entry = (Map.Entry) e;
            json.put(objectToJsonString(entry.getKey()), objectToJsonString(entry.getValue()));
        }
        return json;
    }

    private JSONObject toJsonVertices() {
        JSONObject json = new JSONObject();
        for (Map.Entry<Object, InMemoryVertex> v : this.vertices.entrySet()) {
            json.put(objectToJsonString(v.getKey()), v.getValue().toJson());
        }
        return json;
    }

    private JSONObject toJsonEdges() {
        JSONObject json = new JSONObject();
        for (Map.Entry<Object, InMemoryEdge> e : this.edges.entrySet()) {
            json.put(objectToJsonString(e.getKey()), e.getValue().toJson());
        }
        return json;
    }

    public static InMemoryGraph load(InputStream in) throws IOException {
        String jsonString = StreamUtils.toString(in);
        return load(jsonString);
    }

    public static InMemoryGraph load(String jsonString) {
        return load(new JSONObject(jsonString));
    }

    private static InMemoryGraph load(JSONObject json) {
        Map config = jsonToMap(json.getJSONObject("config"));
        GraphConfiguration configuration = new GraphConfiguration(config);
        IdGenerator idGenerator = configuration.createIdGenerator();
        SearchIndex searchIndex = configuration.createSearchIndex();
        Map<Object, InMemoryVertex> vertices = new HashMap<Object, InMemoryVertex>();
        Map<Object, InMemoryEdge> edges = new HashMap<Object, InMemoryEdge>();
        InMemoryGraph graph = new InMemoryGraph(configuration, idGenerator, searchIndex, vertices, edges);
        loadVertices(graph, json.getJSONObject("vertices"));
        loadEdges(graph, json.getJSONObject("edges"));
        return graph;
    }

    private static void loadVertices(InMemoryGraph graph, JSONObject verticesJson) {
        for (Object keyRaw : verticesJson.keySet()) {
            String keyString = (String) keyRaw;
            Object key = jsonStringToObject(keyString);
            graph.vertices.put(key, InMemoryVertex.fromJson(graph, key, verticesJson.getJSONObject(keyString)));
        }
    }

    private static void loadEdges(InMemoryGraph graph, JSONObject edgesJson) {
        for (Object keyRaw : edgesJson.keySet()) {
            String keyString = (String) keyRaw;
            Object key = jsonStringToObject(keyString);
            graph.edges.put(key, InMemoryEdge.fromJson(graph, key, edgesJson.getJSONObject(keyString)));
        }
    }

    private static Map jsonToMap(JSONObject config) {
        Map map = new HashMap();
        for (Object key : config.keySet()) {
            String keyString = (String) key;
            String val = config.getString(keyString);
            map.put(jsonStringToObject(keyString), jsonStringToObject(val));
        }
        return map;
    }

    static Object jsonStringToObject(String str) {
        if (str.startsWith("base64/java:")) {
            return JavaSerializableUtils.bytesToObject(Base64.decodeBase64(str));
        } else {
            return str;
        }
    }

    static String objectToJsonString(Object value) {
        if (value instanceof String) {
            return (String) value;
        }
        return "base64/java:" + Base64.encodeBase64String(JavaSerializableUtils.objectToBytes(value));
    }
}
