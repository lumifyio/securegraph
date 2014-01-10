package com.altamiracorp.securegraph.type;

import java.io.Serializable;

public class GeoPoint implements Serializable {
    static final long serialVersionUID = 1L;
    private final double latitude;
    private final double longitude;
    private final Double altitude;

    public GeoPoint(double latitude, double longitude, Double altitude) {
        this.latitude = latitude;
        this.longitude = longitude;
        this.altitude = altitude;
    }

    public GeoPoint(double latitude, double longitude) {
        this(latitude, longitude, null);
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
}
