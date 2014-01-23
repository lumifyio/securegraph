package com.altamiracorp.securegraph.inmemory;

import com.altamiracorp.securegraph.Authorizations;

import java.util.Arrays;

public class InMemoryAuthorizations implements Authorizations {
    private final String[] authorizations;

    public InMemoryAuthorizations(String... authorizations) {
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
