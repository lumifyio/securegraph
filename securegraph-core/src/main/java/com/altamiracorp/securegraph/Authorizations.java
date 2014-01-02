package com.altamiracorp.securegraph;

public class Authorizations {
    private final String[] authorizations;

    public Authorizations(String... authorizations) {
        this.authorizations = authorizations;
    }

    public String[] getAuthorizations() {
        return authorizations;
    }
}
