package com.altamiracorp.securegraph.accumulo;

import com.altamiracorp.securegraph.*;
import com.altamiracorp.securegraph.mutation.ExistingElementMutationImpl;
import org.apache.hadoop.io.Text;

import java.io.Serializable;

public abstract class AccumuloElement extends ElementBase implements Serializable {
    private static final long serialVersionUID = 1L;
    public static final Text CF_PROPERTY = new Text("PROP");
    public static final Text CF_PROPERTY_METADATA = new Text("PROPMETA");

    protected AccumuloElement(Graph graph, Object id, Visibility visibility, Iterable<Property> properties) {
        super(graph, id, visibility, properties);
    }

    @Override
    public void removeProperty(String key, String name) {
        Property property = super.removePropertyInternal(key, name);
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

    protected <TElement extends Element> void saveExistingElementMutation(ExistingElementMutationImpl<TElement> mutation) {
        Iterable<Property> properties = mutation.getProperties();
        updatePropertiesInternal(properties);
        getGraph().saveProperties((AccumuloElement) mutation.getElement(), properties);

        if (mutation.getNewElementVisibility() != null) {
            getGraph().alterElementVisibility((AccumuloElement) mutation.getElement(), mutation.getNewElementVisibility());
        }
        getGraph().alterElementPropertyVisibilities((AccumuloElement) mutation.getElement(), mutation.getAlterPropertyVisibilities());
        getGraph().alterPropertyMetadatas((AccumuloElement) mutation.getElement(), mutation.getAlterPropertyMetadatas());
    }
}
