package com.altamiracorp.securegraph.accumulo;

import com.altamiracorp.securegraph.*;
import org.apache.hadoop.io.Text;

import java.util.List;

public abstract class AccumuloElement extends ElementBase {
    public static final Text CF_PROPERTY = new Text("PROP");
    public static final Text CF_PROPERTY_METADATA = new Text("PROPMETA");

    protected AccumuloElement(Graph graph, Object id, Visibility visibility, List<Property> properties) {
        super(graph, id, visibility, properties);
    }

    @Override
    public void removeProperty(String propertyId, String name) {
        Property property = super.removePropertyInternal(propertyId, name);
        if (property != null) {
            getGraph().removeProperty(this, property);
        }
    }

    @Override
    public void removeProperty(String name) {
        Iterable<Property> properties = super.removePropertyInternal(name);
        for (Property property : properties) {
            getGraph().removeProperty(this, property);
        }
    }

    @Override
    public AccumuloGraph getGraph() {
        return (AccumuloGraph) super.getGraph();
    }


    @Override
    public ElementMutation prepareMutation() {
        return new ElementMutation() {
            @Override
            public void save() {
                List<Property> properties = getProperties();
                setPropertiesInternal(properties);
                getGraph().saveProperties(AccumuloElement.this, properties);
            }
        };
    }
}
