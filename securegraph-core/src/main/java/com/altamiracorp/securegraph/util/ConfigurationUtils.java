package com.altamiracorp.securegraph.util;

import com.altamiracorp.securegraph.SecureGraphException;

import java.lang.reflect.Constructor;
import java.util.Map;

public class ConfigurationUtils {
    public static <T> T createProvider(Map config, String propPrefix, String defaultProvider) throws SecureGraphException {
        String implClass = (String) config.get(propPrefix);
        if (implClass == null) {
            implClass = defaultProvider;
        }
        return createProvider(implClass, config);
    }

    @SuppressWarnings("unchecked")
    public static <T> T createProvider(String className, Map config) throws SecureGraphException {
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
}
