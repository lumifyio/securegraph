package com.altamiracorp.securegraph.blueprints;

import com.altamiracorp.securegraph.Authorizations;

public class DefaultAuthorizationsProvider implements AuthorizationsProvider {
    private static final Authorizations DEFAULT_AUTHORIZATIONS = new Authorizations();

    @Override
    public Authorizations getAuthorizations() {
        return DEFAULT_AUTHORIZATIONS;
    }
}
