package com.altamiracorp.securegraph.type;

import com.altamiracorp.securegraph.SecureGraphException;

import java.io.Serializable;

public class GeoCircle implements Serializable, GeoShape {
    static final long serialVersionUID = 1L;
    private final double latitude;
    private final double longitude;
    private final double radius;

    public GeoCircle(double latitude, double longitude, double radius) {
        this.latitude = latitude;
        this.longitude = longitude;
        this.radius = radius;
    }

    public double getLatitude() {
        return latitude;
    }

    public double getLongitude() {
        return longitude;
    }

    public double getRadius() {
        return radius;
    }

    @Override
    public boolean within(GeoShape geoShape) {
        if (geoShape instanceof GeoPoint) {
            GeoPoint pt = (GeoPoint) geoShape;
            return GeoPoint.distanceBetween(getLatitude(), getLongitude(), pt.getLatitude(), pt.getLongitude()) <= getRadius();
        }
        throw new SecureGraphException("Not implemented for argument type " + geoShape.getClass().getName());
    }
}
