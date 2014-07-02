package org.securegraph.query;

import org.securegraph.type.GeoPoint;

public class GeohashBucket {
    public final String key;
    public final long count;
    private final GeoPoint geoPoint;

    public GeohashBucket(String key, long count, GeoPoint geoPoint) {
        this.key = key;
        this.count = count;
        this.geoPoint = geoPoint;
    }

    public String getKey() {
        return key;
    }

    public long getCount() {
        return count;
    }

    public GeoPoint getGeoPoint() {
        return geoPoint;
    }
}
