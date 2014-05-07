package org.securegraph.test.util;

import org.securegraph.Authorizations;
import org.securegraph.Graph;
import org.securegraph.SecureGraphException;

import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;

// This class is only to avoid circular dependencies.
public class GraphRestore {
    public void restore(Graph graph, InputStream in, Authorizations authorizations) {
        try {
            Class clazz = Class.forName("org.securegraph.tools.GraphRestore");
            Constructor constructor = clazz.getConstructor();
            Object graphBackup = constructor.newInstance();
            Method saveMethod = clazz.getMethod("restore", Graph.class, InputStream.class, Authorizations.class);
            saveMethod.invoke(graphBackup, graph, in, authorizations);
        } catch (Exception e) {
            throw new SecureGraphException("Could not find GraphRestore make sure it's in you classpath", e);
        }
    }
}
