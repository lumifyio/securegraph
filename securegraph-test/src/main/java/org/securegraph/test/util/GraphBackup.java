package org.securegraph.test.util;

import org.securegraph.Authorizations;
import org.securegraph.Graph;
import org.securegraph.SecureGraphException;

import java.io.OutputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;

// This class is only to avoid circular dependencies.
public class GraphBackup {
    public void save(Graph graph, OutputStream out, Authorizations authorizations) {
        try {
            Class clazz = Class.forName("org.securegraph.tools.GraphBackup");
            Constructor constructor = clazz.getConstructor();
            Object graphBackup = constructor.newInstance();
            Method saveMethod = clazz.getMethod("save", Graph.class, OutputStream.class, Authorizations.class);
            saveMethod.invoke(graphBackup, graph, out, authorizations);
        } catch (Exception e) {
            throw new SecureGraphException("Could not find GraphBackup make sure it's in you classpath", e);
        }
    }
}
