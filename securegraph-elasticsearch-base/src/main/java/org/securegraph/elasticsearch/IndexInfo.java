package org.securegraph.elasticsearch;

import org.securegraph.PropertyDefinition;

import java.util.HashMap;
import java.util.Map;

public final class IndexInfo {
    private final String indexName;
    private Map<String, PropertyDefinition> propertyDefinitions = new HashMap<String, PropertyDefinition>();
    private boolean elementTypeDefined;

    public IndexInfo(String indexName) {
        this.indexName = indexName;
    }

    public void addPropertyDefinition(String propertyName, PropertyDefinition propertyDefinition) {
        propertyDefinitions.put(propertyName, propertyDefinition);
    }

    public boolean isPropertyDefined(String propertyName) {
        return propertyDefinitions.get(propertyName) != null;
    }

    public String getIndexName() {
        return indexName;
    }

    public Map<String, PropertyDefinition> getPropertyDefinitions() {
        return propertyDefinitions;
    }

    @Override
    public int hashCode() {
        return getIndexName().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof IndexInfo)) {
            return false;
        }
        IndexInfo otherIndexInfo = (IndexInfo) obj;
        return getIndexName().equals(otherIndexInfo.getIndexName());
    }

    public boolean isElementTypeDefined() {
        return elementTypeDefined;
    }

    public void setElementTypeDefined(boolean elementTypeDefined) {
        this.elementTypeDefined = elementTypeDefined;
    }
}
