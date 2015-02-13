package org.securegraph.cli.model;

import org.securegraph.Authorizations;
import org.securegraph.Graph;
import org.securegraph.cli.SecuregraphScript;

public abstract class ModelBase {
    public Graph getGraph() {
        return SecuregraphScript.getGraph();
    }

    public Authorizations getAuthorizations() {
        return SecuregraphScript.getAuthorizations();
    }
}
