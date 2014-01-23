package com.altamiracorp.securegraph.accumulo;

import com.altamiracorp.securegraph.Authorizations;

import java.util.Arrays;

public class AccumuloAuthorizations implements Authorizations {
    private final String[] authorizations;

    public AccumuloAuthorizations(String... authorizations) {
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
