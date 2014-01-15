package com.altamiracorp.securegraph.inmemory;

import com.altamiracorp.securegraph.*;
import com.altamiracorp.securegraph.property.MutableProperty;
import com.altamiracorp.securegraph.property.StreamingPropertyValue;
import com.altamiracorp.securegraph.util.StreamUtils;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class InMemoryElement extends ElementBase {
    protected InMemoryElement(Graph graph, Object id, Visibility visibility, List<Property> properties) {
        super(graph, id, visibility, properties);
    }

    @Override
    public void removeProperty(String propertyId, String name) {
        Property property = removePropertyInternal(propertyId, name);
        if (property != null) {
            getGraph().removeProperty(this, property);
        }
    }

    @Override
    public void removeProperty(String name) {
        Iterable<Property> properties = removePropertyInternal(name);
        for (Property property : properties) {
            getGraph().removeProperty(this, property);
        }
    }

    @Override
    public InMemoryGraph getGraph() {
        return (InMemoryGraph) super.getGraph();
    }

    @Override
    protected void setPropertiesInternal(List<Property> properties) {
        try {
            for (Property property : properties) {
                if (property.getValue() instanceof StreamingPropertyValue) {
                    StreamingPropertyValue value = (StreamingPropertyValue) property.getValue();
                    byte[] valueData = StreamUtils.toBytes(value.getInputStream(null));
                    ((MutableProperty) property).setValue(new InMemoryStreamingPropertyValue(valueData, value.getValueType()));
                }
            }
            super.setPropertiesInternal(properties);
        } catch (IOException ex) {
            throw new SecureGraphException(ex);
        }
    }

    @Override
    protected Iterable<Property> removePropertyInternal(String name) {
        return super.removePropertyInternal(name);
    }

    @Override
    protected Property removePropertyInternal(Object propertyId, String name) {
        return super.removePropertyInternal(propertyId, name);
    }

    public JSONObject toJson() {
        JSONObject json = new JSONObject();
        json.put("visibility", getVisibility().getVisibilityString());
        json.put("properties", toJsonProperties());
        return json;
    }

    private JSONArray toJsonProperties() {
        JSONArray json = new JSONArray();
        for (Property property : getProperties()) {
            json.put(toJsonProperty(property));
        }
        return json;
    }

    private JSONObject toJsonProperty(Property property) {
        JSONObject json = new JSONObject();
        json.put("id", InMemoryGraph.objectToJsonString(property.getId()));
        json.put("name", property.getName());
        json.put("visibility", property.getVisibility().getVisibilityString());
        json.put("value", InMemoryGraph.objectToJsonString(property.getValue()));
        Map<String, Object> metadata = property.getMetadata();
        if (metadata != null) {
            json.put("metadata", toJsonMetadata(metadata));
        }
        return json;
    }

    private JSONObject toJsonMetadata(Map<String, Object> metadata) {
        JSONObject json = new JSONObject();
        for (Map.Entry<String, Object> m : metadata.entrySet()) {
            json.put(m.getKey(), InMemoryGraph.objectToJsonString(m.getValue()));
        }
        return json;
    }

    protected static Visibility fromJsonVisibility(JSONObject jsonObject) {
        return new Visibility(jsonObject.getString("visibility"));
    }

    protected static List<Property> fromJsonProperties(JSONObject jsonObject) {
        JSONArray propertiesJson = jsonObject.getJSONArray("properties");
        List<Property> properties = new ArrayList<Property>();
        for (int i = 0; i < propertiesJson.length(); i++) {
            JSONObject propertyJson = propertiesJson.getJSONObject(i);
            properties.add(fromJsonProperty(propertyJson));
        }
        return properties;
    }

    private static Property fromJsonProperty(JSONObject propertyJson) {
        Object id = InMemoryGraph.jsonStringToObject(propertyJson.getString("id"));
        String name = propertyJson.getString("name");
        Object value = InMemoryGraph.jsonStringToObject(propertyJson.getString("value"));
        Map<String, Object> metadata = fromJsonPropertyMetadata(propertyJson.optJSONObject("metadata"));
        Visibility visibility = new Visibility(propertyJson.getString("visibility"));
        return new MutableProperty(id, name, value, metadata, visibility);
    }

    private static Map<String, Object> fromJsonPropertyMetadata(JSONObject metadataJson) {
        Map<String, Object> metadata = new HashMap<String, Object>();
        if (metadataJson == null) {
            return metadata;
        }
        for (Object key : metadataJson.keySet()) {
            String keyString = (String) key;
            Object val = InMemoryGraph.jsonStringToObject(metadataJson.getString(keyString));
            metadata.put(keyString, val);
        }
        return metadata;
    }

    @Override
    public ElementMutation prepareMutation() {
        return new ElementMutation() {
            @Override
            public void save() {
                List<Property> properties = getProperties();
                setPropertiesInternal(properties);
                getGraph().saveProperties(InMemoryElement.this, properties);
            }
        };
    }
}
