package com.altamiracorp.securegraph.accumulo.blueprints;

import com.altamiracorp.securegraph.SecureGraphException;
import com.altamiracorp.securegraph.accumulo.AccumuloGraph;
import com.altamiracorp.securegraph.blueprints.*;
import com.altamiracorp.securegraph.util.ConfigurationUtils;
import com.altamiracorp.securegraph.util.MapUtils;

import java.util.Map;

public class AccumuloSecureGraphBlueprintsGraphFactory extends SecureGraphBlueprintsFactory {
    @Override
    protected SecureGraphBlueprintsGraph createGraph(Map config) {
        AccumuloGraph graph = createAccumuloGraph(config);
        VisibilityProvider visibilityProvider = createVisibilityProvider(config);
        AuthorizationsProvider authorizationProvider = createAuthorizationsProvider(config);
        return new AccumuloSecureGraphBlueprintsGraph(graph, visibilityProvider, authorizationProvider);
    }

    private AccumuloGraph createAccumuloGraph(Map config) {
        try {
            Map graphConfig = MapUtils.getAllWithPrefix(config, "graph");
            return AccumuloGraph.create(graphConfig);
        } catch (Exception ex) {
            throw new SecureGraphException("Could not create accumulo graph", ex);
        }
    }

    private VisibilityProvider createVisibilityProvider(Map config) {
        try {
            return ConfigurationUtils.createProvider(config, "visibilityProvider", DefaultVisibilityProvider.class.getName());
        } catch (Exception ex) {
            throw new SecureGraphException("Could not create visibility provider", ex);
        }
    }

    private AuthorizationsProvider createAuthorizationsProvider(Map config) {
        try {
            return ConfigurationUtils.createProvider(config, "authorizationsProvider", DefaultAuthorizationsProvider.class.getName());
        } catch (Exception ex) {
            throw new SecureGraphException("Could not create authorization provider", ex);
        }
    }
}
