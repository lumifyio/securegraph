package org.securegraph.query;

public class TermsQueryItem {
    private final String name;
    private final String fieldName;

    public TermsQueryItem(String name, String fieldName) {
        this.name = name;
        this.fieldName = fieldName;
    }

    public String getName() {
        return name;
    }

    public String getFieldName() {
        return fieldName;
    }
}
