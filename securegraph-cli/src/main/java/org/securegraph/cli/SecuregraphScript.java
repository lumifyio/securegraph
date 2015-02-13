package org.securegraph.cli;

import groovy.lang.Script;
import org.securegraph.Authorizations;
import org.securegraph.Graph;
import org.securegraph.cli.model.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SecuregraphScript extends Script {
    private static Graph graph;
    private static Authorizations authorizations;
    private static final Map<String, LazyProperty> contextProperties = new HashMap<>();
    private static final Map<String, LazyEdge> contextEdges = new HashMap<>();
    private static final Map<String, LazyVertex> contextVertices = new HashMap<>();

    public static void setGraph(Graph graph) {
        SecuregraphScript.graph = graph;
    }

    public Graph getGraph() {
        return graph;
    }

    @Override
    public Object run() {
        return null;
    }

    @Override
    public Object invokeMethod(String name, Object args) {
        if ("setauths".equalsIgnoreCase(name)) {
            return invokeSetAuths(args);
        }

        if ("getauths".equalsIgnoreCase(name)) {
            return invokeGetAuths();
        }

        return super.invokeMethod(name, args);
    }

    private Object invokeGetAuths() {
        return getAuthorizations();
    }

    private Object invokeSetAuths(Object args) {
        String[] auths = invokeMethodArgsToStrings(args);
        setAuthorizations(getGraph().createAuthorizations(auths));
        return invokeGetAuths();
    }

    private String[] invokeMethodArgsToStrings(Object args) {
        if (args == null) {
            return new String[0];
        }
        Object[] authsObjects = (Object[]) args;
        List<String> authsList = new ArrayList<>();
        for (Object authObject : authsObjects) {
            authsList.add(authObject.toString());
        }
        return authsList.toArray(new String[authsList.size()]);
    }

    @Override
    public Object getProperty(String property) {
        if ("v".equals(property)) {
            return new LazyVertexMap(this);
        }

        if ("e".equals(property)) {
            return new LazyEdgeMap(this);
        }

        if ("g".equals(property)) {
            return getGraph();
        }

        if ("setauths".equalsIgnoreCase(property)) {
            return invokeSetAuths(null);
        }

        if ("getauths".equalsIgnoreCase(property)) {
            return invokeGetAuths();
        }

        Object contextProperty = contextProperties.get(property);
        if (contextProperty != null) {
            return contextProperty;
        }

        Object contextEdge = contextEdges.get(property);
        if (contextEdge != null) {
            return contextEdge;
        }

        Object contextVertex = contextVertices.get(property);
        if (contextVertex != null) {
            return contextVertex;
        }

        return super.getProperty(property);
    }

    public Authorizations getAuthorizations() {
        if (authorizations == null) {
            authorizations = getGraph().createAuthorizations();
        }
        return authorizations;
    }

    public static void setAuthorizations(Authorizations authorizations) {
        SecuregraphScript.authorizations = authorizations;
    }

    public Map<String, LazyProperty> getContextProperties() {
        return contextProperties;
    }

    public Map<String, LazyEdge> getContextEdges() {
        return contextEdges;
    }

    public Map<String, LazyVertex> getContextVertices() {
        return contextVertices;
    }
}
