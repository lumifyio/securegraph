package org.securegraph.blueprints;

import org.securegraph.SecureGraphException;
import org.securegraph.util.MapUtils;

import java.io.FileInputStream;
import java.lang.reflect.Constructor;
import java.util.Map;
import java.util.Properties;

public abstract class SecureGraphBlueprintsFactory {
    public static final String STORAGE_CONFIG_PREFIX = "storage";

    public static SecureGraphBlueprintsGraph open(String configFileName) throws Exception {
        try {
            FileInputStream in = new FileInputStream(configFileName);
            try {
                Properties properties = new Properties();
                properties.load(in);

                String storageFactoryClassName = properties.getProperty(STORAGE_CONFIG_PREFIX);
                Map storageConfig = MapUtils.getAllWithPrefix(properties, STORAGE_CONFIG_PREFIX);
                return createFactory(storageFactoryClassName).createGraph(storageConfig);
            } finally {
                in.close();
            }
        } catch (Exception ex) {
            ex.printStackTrace(System.err);
            throw ex;
        }
    }

    protected abstract SecureGraphBlueprintsGraph createGraph(Map config);

    private static SecureGraphBlueprintsFactory createFactory(String factoryClassName) {
        try {
            Class factoryClass = Class.forName(factoryClassName);
            Constructor constructor = factoryClass.getConstructor();
            return (SecureGraphBlueprintsFactory) constructor.newInstance();
        } catch (Exception ex) {
            throw new SecureGraphException("Could not create factory: " + factoryClassName, ex);
        }
    }
}
