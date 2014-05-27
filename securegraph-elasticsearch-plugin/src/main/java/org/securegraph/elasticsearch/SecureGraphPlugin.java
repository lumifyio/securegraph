package org.securegraph.elasticsearch;

import org.elasticsearch.common.inject.Module;
import org.elasticsearch.indices.query.IndicesQueriesModule;
import org.elasticsearch.plugins.AbstractPlugin;

public class SecureGraphPlugin extends AbstractPlugin {
    @Override
    public String name() {
        return "securegraph-plugin";
    }

    @Override
    public String description() {
        return "SecureGraph plugin for applying security filters.";
    }

    @Override
    public void processModule(Module module) {
        if (module instanceof IndicesQueriesModule) {
            ((IndicesQueriesModule) module).addFilter(new AuthorizationsFilterParser());
        }
    }
}
