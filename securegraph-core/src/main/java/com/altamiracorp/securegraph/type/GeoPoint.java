package com.altamiracorp.securegraph.type;

import com.altamiracorp.securegraph.SecureGraphException;

import java.io.Serializable;

public class GeoPoint implements Serializable, GeoShape {
    private static final long serialVersionUID = 1L;
    private static double EARTH_RADIUS = 6371; // km
    private final double latitude;
    private final double longitude;
    private final Double altitude;
    private final String description;

    public GeoPoint(double latitude, double longitude, Double altitude, String description) {
        this.latitude = latitude;
        this.longitude = longitude;
        this.altitude = altitude;
        this.description = description;
    }

    public GeoPoint(double latitude, double longitude, Double altitude) {
        this(latitude, longitude, altitude, null);
    }

    public GeoPoint(double latitude, double longitude) {
        this(latitude, longitude, null, null);
    }

    public GeoPoint(double latitude, double longitude, String description) {
        this(latitude, longitude, null, description);
    }

    public double getLatitude() {
        return latitude;
    }

    public double getLongitude() {
        return longitude;
    }

    public Double getAltitude() {
        return altitude;
    }

    public String getDescription() {
        return description;
    }

    @Override
    public boolean within(GeoShape geoShape) {
        throw new SecureGraphException("Not implemented for argument type " + geoShape.getClass().getName());
    }

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 47 * hash + (int) (Double.doubleToLongBits(this.latitude) ^ (Double.doubleToLongBits(this.latitude) >>> 32));
        hash = 47 * hash + (int) (Double.doubleToLongBits(this.longitude) ^ (Double.doubleToLongBits(this.longitude) >>> 32));
        hash = 47 * hash + (this.altitude != null ? this.altitude.hashCode() : 0);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final GeoPoint other = (GeoPoint) obj;
        if (Double.doubleToLongBits(this.latitude) != Double.doubleToLongBits(other.latitude)) {
            return false;
        }
        if (Double.doubleToLongBits(this.longitude) != Double.doubleToLongBits(other.longitude)) {
            return false;
        }
        if (this.altitude != other.altitude && (this.altitude == null || !this.altitude.equals(other.altitude))) {
            return false;
        }
        return true;
    }

    // see http://www.movable-type.co.uk/scripts/latlong.html
    public static double distanceBetween(double latitude1, double longitude1, double latitude2, double longitude2) {
        double dLat = toRadians(latitude2 - latitude1);
        double dLon = toRadians(longitude2 - longitude1);
        latitude1 = toRadians(latitude1);
        latitude2 = toRadians(latitude2);

        double a = Math.sin(dLat / 2) * Math.sin(dLat / 2) + Math.sin(dLon / 2) * Math.sin(dLon / 2) * Math.cos(latitude1) * Math.cos(latitude2);
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        return EARTH_RADIUS * c;
    }

    private static double toRadians(double v) {
        return v * Math.PI / 180;
    }
}
