package org.securegraph.event;

import org.securegraph.Element;
import org.securegraph.Graph;
import org.securegraph.Property;

public class RemovePropertyEvent extends GraphEvent {
    private final Element element;
    private final Property property;

    public RemovePropertyEvent(Graph graph, Element element, Property property) {
        super(graph);
        this.element = element;
        this.property = property;
    }

    public Element getElement() {
        return element;
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
        return "RemovePropertyEvent{element=" + getElement() + ", property=" + property + '}';
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof RemovePropertyEvent)) {
            return false;
        }

        RemovePropertyEvent other = (RemovePropertyEvent) obj;
        return getElement().equals(other.getElement())
                && getProperty().equals(other.getProperty())
                && super.equals(obj);
    }
}
