package com.altamiracorp.securegraph.accumulo;

import com.altamiracorp.securegraph.ElementBase;
import com.altamiracorp.securegraph.Graph;
import com.altamiracorp.securegraph.Property;
import com.altamiracorp.securegraph.Visibility;
import org.apache.hadoop.io.Text;

public abstract class AccumuloElement extends ElementBase {
    public static final Text CF_PROPERTY = new Text("PROP");

    protected AccumuloElement(Graph graph, Object id, Visibility visibility) {
        super(graph, id, visibility);
    }

    @Override
    public Iterable<Property> getProperties() {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void setProperties(Property... properties) {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void addProperties(Property... properties) {
        throw new RuntimeException("not implemented");
    }
}
