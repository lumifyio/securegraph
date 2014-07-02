package org.securegraph.query;

public class GeohashResult {
    private final Iterable<GeohashBucket> buckets;

    public GeohashResult(Iterable<GeohashBucket> buckets) {
        this.buckets = buckets;
    }

    public Iterable<GeohashBucket> getBuckets() {
        return buckets;
    }
}
