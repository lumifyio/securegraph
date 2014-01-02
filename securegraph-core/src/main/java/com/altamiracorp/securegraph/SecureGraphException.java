package com.altamiracorp.securegraph;

public class SecureGraphException extends RuntimeException {
    public SecureGraphException(Exception e) {
        super(e);
    }

    public SecureGraphException(String msg, Exception e) {
        super(msg, e);
    }

    public SecureGraphException(String msg) {
        super(msg);
    }
}
