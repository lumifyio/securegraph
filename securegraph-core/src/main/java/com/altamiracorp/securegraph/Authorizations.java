package com.altamiracorp.securegraph;

import java.util.Arrays;

public class Authorizations {
    private final String[] authorizations;

    public Authorizations(String... authorizations) {
        this.authorizations = authorizations;
    }

    public String[] getAuthorizations() {
        return authorizations;
    }

    @Override
    public String toString() {
        return Arrays.toString(authorizations);
    }
}
