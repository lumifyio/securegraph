package com.altamiracorp.securegraph;

import com.altamiracorp.securegraph.id.IdGenerator;
import com.altamiracorp.securegraph.id.UUIDIdGenerator;
import com.altamiracorp.securegraph.search.DefaultSearchIndex;
import com.altamiracorp.securegraph.search.SearchIndex;
import com.altamiracorp.securegraph.util.ConfigurationUtils;

import java.util.Map;

public class GraphConfiguration {
    public static final String IDGENERATOR_PROP_PREFIX = "idgenerator";
    public static final String SEARCH_INDEX_PROP_PREFIX = "search";

    public static final String DEFAULT_IDGENERATOR = UUIDIdGenerator.class.getName();
    public static final String DEFAULT_SEARCH_INDEX = DefaultSearchIndex.class.getName();

    private final Map config;

    public GraphConfiguration(Map config) {
        this.config = config;
    }

    public void set(String key, Object value) {
        this.config.put(key, value);
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

    public IdGenerator createIdGenerator() throws SecureGraphException {
        return ConfigurationUtils.createProvider(getConfig(), IDGENERATOR_PROP_PREFIX, DEFAULT_IDGENERATOR);
    }

    public SearchIndex createSearchIndex() throws SecureGraphException {
        return ConfigurationUtils.createProvider(getConfig(), SEARCH_INDEX_PROP_PREFIX, DEFAULT_SEARCH_INDEX);
    }
}
