package org.securegraph.event;

import org.securegraph.Graph;
import org.securegraph.Property;

public class AddPropertyEvent extends GraphEvent {
    private final Property property;

    public AddPropertyEvent(Graph graph, Thread thread, Property property) {
        super(graph, thread);
        this.property = property;
    }

    public Property getProperty() {
        return property;
    }

    @Override
    public int hashCode() {
        return getProperty().hashCode();
    }

    @Override
    public String toString() {
        return "AddPropertyEvent{property=" + property + '}';
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof AddPropertyEvent) {
            AddPropertyEvent other = (AddPropertyEvent) obj;
            if (!getProperty().equals(other.getProperty())) {
                return false;
            }
        }
        return super.equals(obj);
    }
}
