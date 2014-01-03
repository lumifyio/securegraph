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
        ((AccumuloGraph) getGraph()).saveProperties(this, properties);
        super.setPropertiesInternal(properties);
    }
}
