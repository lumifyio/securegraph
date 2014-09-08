package org.securegraph.elasticsearch;

import org.securegraph.*;
import org.securegraph.mutation.AlterPropertyMetadata;
import org.securegraph.mutation.AlterPropertyVisibility;
import org.securegraph.mutation.ExistingElementMutation;
import org.securegraph.mutation.ExistingElementMutationImpl;
import org.securegraph.property.MutableProperty;

import java.io.Serializable;

public class ElasticSearchElement<T extends Element> extends ElementBase<T> implements Serializable {
    protected ElasticSearchElement(Graph graph, String id, Visibility visibility, Iterable<Property> properties, Authorizations authorizations) {
        super(graph, id, visibility, properties, authorizations);
    }

    @Override
    public ExistingElementMutation<T> prepareMutation() {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void removeProperty(String key, String name, Authorizations authorizations) {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void removeProperty(String name, Authorizations authorizations) {
        throw new RuntimeException("not implemented");
    }

    protected <TElement extends Element> void saveExistingElementMutation(ExistingElementMutationImpl<TElement> mutation, Authorizations authorizations) {
        // Order matters a lot here
        ElasticSearchElement element = (ElasticSearchElement) mutation.getElement();

        // metadata must be altered first because the lookup of a property can include visibility which will be altered by alterElementPropertyVisibilities
        for (AlterPropertyMetadata apm : mutation.getAlterPropertyMetadatas()) {
            Property property = element.getProperty(apm.getPropertyKey(), apm.getPropertyName(), apm.getPropertyVisibility());
            if (property == null) {
                throw new SecureGraphException(String.format("Could not find property %s:%s(%s)", apm.getPropertyKey(), apm.getPropertyName(), apm.getPropertyVisibility()));
            }
            property.getMetadata().put(apm.getMetadataName(), apm.getNewValue());
        }

        // altering properties comes next because alterElementVisibility may alter the vertex and we won't find it
        for (AlterPropertyVisibility apv : mutation.getAlterPropertyVisibilities()) {
            MutableProperty property = (MutableProperty) element.getProperty(apv.getKey(), apv.getName(), apv.getExistingVisibility());
            if (property == null) {
                throw new SecureGraphException("Could not find property " + apv.getKey() + ":" + apv.getName());
            }
            if (property.getVisibility().equals(apv.getVisibility())) {
                continue;
            }
            property.setVisibility(apv.getVisibility());
        }

        Iterable<Property> properties = mutation.getProperties();
        updatePropertiesInternal(properties);

        if (mutation.getNewElementVisibility() != null) {
            element.setVisibility(mutation.getNewElementVisibility());
        }
    }
}
