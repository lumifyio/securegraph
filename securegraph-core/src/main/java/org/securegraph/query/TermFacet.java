package org.securegraph.query;

public class TermFacet extends Facet {
    private final String name;
    private final String propertyName;

    public TermFacet(String name, String propertyName) {
        this.name = name;
        this.propertyName = propertyName;
    }

    public String getName() {
        return name;
    }

    public String getPropertyName() {
        return propertyName;
    }
}
