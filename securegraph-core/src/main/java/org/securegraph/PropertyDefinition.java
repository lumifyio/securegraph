package org.securegraph;

import java.util.Set;

public class PropertyDefinition {
    private final String propertyName;
    private final Class dataType;
    private final Set<TextIndexHint> textIndexHints;

    public PropertyDefinition(String propertyName, Class dataType, Set<TextIndexHint> textIndexHints) {
        this.propertyName = propertyName;
        this.dataType = dataType;
        this.textIndexHints = textIndexHints;
    }

    public String getPropertyName() {
        return propertyName;
    }

    public Class getDataType() {
        return dataType;
    }

    public Set<TextIndexHint> getTextIndexHints() {
        return textIndexHints;
    }
}
