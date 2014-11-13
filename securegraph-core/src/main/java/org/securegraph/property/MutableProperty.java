package org.securegraph.property;

import org.securegraph.Property;
import org.securegraph.Visibility;

public abstract class MutableProperty extends Property {
    public abstract void setValue(Object value);

    public abstract void setVisibility(Visibility visibility);

    public abstract void addHiddenVisibility(Visibility visibility);
}
