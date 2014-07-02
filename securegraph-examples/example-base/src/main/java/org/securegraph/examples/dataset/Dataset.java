package org.securegraph.examples.dataset;

import org.securegraph.Authorizations;
import org.securegraph.Graph;

import java.io.IOException;

public abstract class Dataset {
    public abstract void load(Graph graph, int numberOfVerticesToCreate, String[] visibilities, Authorizations authorizations) throws IOException;
}
