package org.securegraph.query;

public class HistogramQueryItem {
    private final String name;
    private final String fieldName;
    private final String interval;

    public HistogramQueryItem(String name, String fieldName, String interval) {
        this.name = name;
        this.fieldName = fieldName;
        this.interval = interval;
    }

    public String getName() {
        return name;
    }

    public String getFieldName() {
        return fieldName;
    }

    public String getInterval() {
        return interval;
    }
}
