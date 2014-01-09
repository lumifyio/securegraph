package com.altamiracorp.securegraph;

import java.lang.reflect.Method;
import java.util.Map;

public class GraphFactory {
    public Graph createGraph(Map config) {
        String graphClassName = (String) config.get("");
        try {
            Class graphClass = Class.forName(graphClassName);
            try {
                Method createMethod = graphClass.getDeclaredMethod("create", Map.class);
                try {
                    return (Graph) createMethod.invoke(null, config);
                } catch (Exception e) {
                    throw new SecureGraphException("Failed on " + createMethod + " on class " + graphClass.getName(), e);
                }
            } catch (NoSuchMethodException e) {
                throw new SecureGraphException("Could not find create(java.lang.Map) method on class " + graphClass.getName(), e);
            }
        } catch (ClassNotFoundException e) {
            throw new SecureGraphException("Could not find class: " + graphClassName, e);
        }
    }
}
