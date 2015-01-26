package org.securegraph.accumulo.blueprints;

import org.securegraph.GraphConfiguration;
import org.securegraph.SecureGraphException;
import org.securegraph.accumulo.AccumuloGraph;
import org.securegraph.blueprints.*;
import org.securegraph.util.ConfigurationUtils;
import org.securegraph.util.MapUtils;

import java.util.Map;

public class AccumuloSecureGraphBlueprintsGraphFactory extends SecureGraphBlueprintsFactory {
    @Override
    public SecureGraphBlueprintsGraph createGraph(Map config) {
        AccumuloGraph graph = createAccumuloGraph(config);
        VisibilityProvider visibilityProvider = createVisibilityProvider(graph.getConfiguration());
        AuthorizationsProvider authorizationProvider = createAuthorizationsProvider(graph.getConfiguration());
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

    private VisibilityProvider createVisibilityProvider(GraphConfiguration config) {
        try {
            return ConfigurationUtils.createProvider(config, "visibilityProvider", DefaultVisibilityProvider.class.getName());
        } catch (Exception ex) {
            throw new SecureGraphException("Could not create visibility provider", ex);
        }
    }

    private AuthorizationsProvider createAuthorizationsProvider(GraphConfiguration config) {
        try {
            return ConfigurationUtils.createProvider(config, "authorizationsProvider", null);
        } catch (Exception ex) {
            throw new SecureGraphException("Could not create authorization provider", ex);
        }
    }
}
