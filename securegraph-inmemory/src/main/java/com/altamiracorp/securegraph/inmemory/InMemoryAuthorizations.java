package com.altamiracorp.securegraph.inmemory;

import com.altamiracorp.securegraph.Authorizations;

import java.io.Serializable;
import java.util.Arrays;

public class InMemoryAuthorizations implements Authorizations, Serializable {
    private static final long serialVersionUID = 1L;
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
