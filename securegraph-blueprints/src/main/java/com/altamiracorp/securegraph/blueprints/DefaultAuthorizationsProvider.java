package com.altamiracorp.securegraph.blueprints;

import com.altamiracorp.securegraph.Authorizations;

import java.util.Map;

public class DefaultAuthorizationsProvider implements AuthorizationsProvider {
    private static final Authorizations DEFAULT_AUTHORIZATIONS = new Authorizations();

    public DefaultAuthorizationsProvider(Map config) {

    }

    @Override
    public Authorizations getAuthorizations() {
        return DEFAULT_AUTHORIZATIONS;
    }
}
