package com.altamiracorp.securegraph.accumulo;

import com.altamiracorp.securegraph.ElementBase;
import com.altamiracorp.securegraph.Graph;
import com.altamiracorp.securegraph.Property;
import com.altamiracorp.securegraph.Visibility;
import org.apache.hadoop.io.Text;

public abstract class AccumuloElement extends ElementBase {
    public static final Text CF_PROPERTY = new Text("PROP");
    public static final Text CF_PROPERTY_METADATA = new Text("PROPMETA");

    protected AccumuloElement(Graph graph, Object id, Visibility visibility, Property[] properties) {
        super(graph, id, visibility, properties);
    }

    @Override
    public void setProperties(Property... properties) {
        super.setPropertiesInternal(properties);
        getGraph().saveProperties(this, properties);
    }

    @Override
    public void removeProperty(String propertyId, String name) {
        Property property = super.removePropertyInternal(propertyId, name);
        if (property != null) {
            getGraph().removeProperty(this, property);
        }
    }

    @Override
    public AccumuloGraph getGraph() {
        return (AccumuloGraph) super.getGraph();
    }
}
