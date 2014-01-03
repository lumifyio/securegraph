package com.altamiracorp.securegraph.accumulo;

import com.altamiracorp.securegraph.GraphConfiguration;
import com.altamiracorp.securegraph.SecureGraphException;
import com.altamiracorp.securegraph.search.DefaultSearchIndex;
import com.altamiracorp.securegraph.search.SearchIndex;
import com.altamiracorp.securegraph.accumulo.serializer.JavaValueSerializer;
import com.altamiracorp.securegraph.accumulo.serializer.ValueSerializer;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;

import java.util.Map;

public class AccumuloGraphConfiguration extends GraphConfiguration {
    public static final String ACCUMULO_INSTANCE_NAME = "accumulo.instance";
    public static final String ZOOKEEPER_SERVERS = "zookeeper.servers";
    public static final String ACCUMULO_USERNAME = "accumulo.username";
    public static final String ACCUMULO_PASSWORD = "accumulo.password";
    public static final String VALUE_SERIALIZER_PROP_PREFIX = "serializer";
    public static final String AUTO_FLUSH = "autoflush";
    public static final String TABLE_NAME = "tablename";

    public static final String DEFAULT_ACCUMULO_PASSWORD = "password";
    public static final String DEFAULT_VALUE_SERIALIZER = JavaValueSerializer.class.getName();
    public static final String DEFAULT_ACCUMULO_USERNAME = "root";
    public static final String DEFAULT_ACCUMULO_INSTANCE_NAME = "blueprints_accumulo";
    public static final String DEFAULT_ZOOKEEPER_SERVERS = "localhost";
    public static final boolean DEFAULT_AUTO_FLUSH = false;
    public static final String DEFAULT_TABLE_NAME = "securegraph";

    public AccumuloGraphConfiguration(Map config) {
        super(config);
    }

    public Connector createConnector() throws AccumuloSecurityException, AccumuloException {
        ZooKeeperInstance instance = new ZooKeeperInstance(this.getAccumuloInstanceName(), this.getZookeeperServers());
        return instance.getConnector(this.getAccumuloUsername(), this.getAuthenticationToken());
    }

    private AuthenticationToken getAuthenticationToken() {
        String password = getConfigString(ACCUMULO_PASSWORD, DEFAULT_ACCUMULO_PASSWORD);
        return new PasswordToken(password);
    }

    private String getAccumuloUsername() {
        return getConfigString(ACCUMULO_USERNAME, DEFAULT_ACCUMULO_USERNAME);
    }

    public String getAccumuloInstanceName() {
        return getConfigString(ACCUMULO_INSTANCE_NAME, DEFAULT_ACCUMULO_INSTANCE_NAME);
    }

    public String getZookeeperServers() {
        return getConfigString(ZOOKEEPER_SERVERS, DEFAULT_ZOOKEEPER_SERVERS);
    }

    public ValueSerializer createValueSerializer() throws SecureGraphException {
        return this.createProvider(VALUE_SERIALIZER_PROP_PREFIX, DEFAULT_VALUE_SERIALIZER);
    }

    public boolean isAutoFlush() {
        return getConfigBoolean(AUTO_FLUSH, DEFAULT_AUTO_FLUSH);
    }

    public String getTableName() {
        return getConfigString(TABLE_NAME, DEFAULT_TABLE_NAME);
    }
}
