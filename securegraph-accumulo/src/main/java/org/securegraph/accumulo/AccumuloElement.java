package org.securegraph.accumulo;

import org.securegraph.*;
import org.securegraph.mutation.ExistingElementMutationImpl;
import org.apache.hadoop.io.Text;

import java.io.Serializable;

public abstract class AccumuloElement<T extends Element> extends ElementBase<T> implements Serializable {
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
        // Order matters a lot here

        // metadata must be altered first because the lookup of a property can include visibility which will be altered by alterElementPropertyVisibilities
        getGraph().alterPropertyMetadatas((AccumuloElement) mutation.getElement(), mutation.getAlterPropertyMetadatas());

        // altering properties comes next because alterElementVisibility may alter the vertex and we won't find it
        getGraph().alterElementPropertyVisibilities((AccumuloElement) mutation.getElement(), mutation.getAlterPropertyVisibilities());

        Iterable<Property> properties = mutation.getProperties();
        updatePropertiesInternal(properties);
        getGraph().saveProperties((AccumuloElement) mutation.getElement(), properties);

        if (mutation.getNewElementVisibility() != null) {
            getGraph().alterElementVisibility((AccumuloElement) mutation.getElement(), mutation.getNewElementVisibility());
        }
    }
}
