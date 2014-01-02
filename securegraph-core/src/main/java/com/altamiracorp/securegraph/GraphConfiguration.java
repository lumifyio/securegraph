package com.altamiracorp.securegraph;

import com.altamiracorp.securegraph.id.IdGenerator;
import com.altamiracorp.securegraph.id.UUIDIdGenerator;

import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.Map;

public abstract class GraphConfiguration {
    public static final String IDGENERATOR_PROP_PREFIX = "idgenerator";
    public static final String DEFAULT_IDGENERATOR = UUIDIdGenerator.class.getName();

    private final Map config;

    public GraphConfiguration(Map config) {
        this.config = config;
    }

    public Map getConfig() {
        return config;
    }

    public String getConfigString(String key, String defaultValue) {
        Object o = getConfig(key, defaultValue);
        if (o == null) {
            return null;
        }
        return o.toString();
    }

    public boolean getConfigBoolean(String key, boolean defaultValue) {
        Object o = getConfig(key, defaultValue);
        if (o instanceof Boolean) {
            return (Boolean) o;
        }
        return Boolean.valueOf(o.toString());
    }

    @SuppressWarnings("unchecked")
    public Object getConfig(String key, Object defaultValue) {
        Object o = getConfig().get(key);
        if (o == null) {
            return defaultValue;
        }
        return o;
    }

    @SuppressWarnings("unchecked")
    private Map getConfigSubset(String prefix) {
        Map result = new HashMap();
        for (Object e : getConfig().entrySet()) {
            Map.Entry entry = (Map.Entry) e;
            if (entry.getKey().equals(prefix)) {
                result.put(entry.getKey(), entry.getValue());
            }
        }
        return result;
    }

    public <T> T createProvider(String propPrefix, String defaultProvider) throws SecureGraphException {
        Map subsetConfig = getConfigSubset(propPrefix);
        String implClass = getConfigString(propPrefix, defaultProvider);
        return this.createProvider(implClass, subsetConfig);
    }

    @SuppressWarnings("unchecked")
    private <T> T createProvider(String className, Map config) throws SecureGraphException {
        Class<Map> constructorParameterClass = Map.class;
        try {
            Class<?> clazz = Class.forName(className);
            try {
                Constructor constructor = clazz.getConstructor(constructorParameterClass);
                return (T) constructor.newInstance(config);
            } catch (IllegalArgumentException e) {
                StringBuilder possibleMatches = new StringBuilder();
                for (Constructor<?> s : clazz.getConstructors()) {
                    possibleMatches.append(s.toGenericString());
                    possibleMatches.append(", ");
                }
                throw new SecureGraphException("Invalid constructor for " + className + ". Expected <init>(" + constructorParameterClass.getName() + "). Found: " + possibleMatches, e);
            }
        } catch (NoSuchMethodException e) {
            throw new SecureGraphException("Provider must have a single argument constructor taking a " + constructorParameterClass.getName(), e);
        } catch (ClassNotFoundException e) {
            throw new SecureGraphException("No provider found with class name " + className, e);
        } catch (Exception e) {
            throw new SecureGraphException(e);
        }
    }

    public IdGenerator createIdGenerator() throws SecureGraphException {
        return this.createProvider(IDGENERATOR_PROP_PREFIX, DEFAULT_IDGENERATOR);
    }
}
