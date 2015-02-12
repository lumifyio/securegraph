package org.securegraph.cli.model;

import org.securegraph.Authorizations;
import org.securegraph.Graph;
import org.securegraph.cli.SecuregraphScript;

public abstract class ModelBase {
    private final SecuregraphScript script;

    public ModelBase(SecuregraphScript script) {
        this.script = script;
    }

    public Graph getGraph() {
        return getScript().getGraph();
    }

    public SecuregraphScript getScript() {
        return script;
    }

    public Authorizations getAuthorizations() {
        return getScript().getAuthorizations();
    }

    protected String valueToString(Object value) {
        return value.toString();
    }
}
