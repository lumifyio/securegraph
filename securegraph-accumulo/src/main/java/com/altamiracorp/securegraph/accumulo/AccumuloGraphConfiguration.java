package com.altamiracorp.securegraph.accumulo;

import com.altamiracorp.securegraph.GraphConfiguration;
import com.altamiracorp.securegraph.SecureGraphException;
import com.altamiracorp.securegraph.accumulo.serializer.JavaValueSerializer;
import com.altamiracorp.securegraph.accumulo.serializer.ValueSerializer;
import com.altamiracorp.securegraph.util.ConfigurationUtils;
import com.altamiracorp.securegraph.util.MapUtils;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

public class AccumuloGraphConfiguration extends GraphConfiguration {
    private static final Logger LOGGER = LoggerFactory.getLogger(AccumuloGraphConfiguration.class);

    public static final String HDFS_CONFIG_PREFIX = "hdfs";

    public static final String ACCUMULO_INSTANCE_NAME = "accumuloInstanceName";
    public static final String ACCUMULO_USERNAME = "username";
    public static final String ACCUMULO_PASSWORD = "password";
    public static final String ZOOKEEPER_SERVERS = "zookeeperServers";
    public static final String VALUE_SERIALIZER_PROP_PREFIX = "serializer";
    public static final String AUTO_FLUSH = "autoFlush";
    public static final String TABLE_NAME = "tableName";
    public static final String MAX_STREAMING_PROPERTY_VALUE_TABLE_DATA_SIZE = "maxStreamingPropertyValueTableDataSize";
    public static final String HDFS_USER = HDFS_CONFIG_PREFIX + ".user";
    public static final String HDFS_ROOT_DIR = HDFS_CONFIG_PREFIX + ".rootDir";
    public static final String DATA_DIR = HDFS_CONFIG_PREFIX + ".dataDir";
    public static final String USE_SERVER_SIDE_ELEMENT_VISIBILITY_ROW_FILTER = "useServerSideElementVisibilityRowFilter";

    public static final String DEFAULT_ACCUMULO_PASSWORD = "password";
    public static final String DEFAULT_VALUE_SERIALIZER = JavaValueSerializer.class.getName();
    public static final String DEFAULT_ACCUMULO_USERNAME = "root";
    public static final String DEFAULT_ACCUMULO_INSTANCE_NAME = "securegraph";
    public static final String DEFAULT_ZOOKEEPER_SERVERS = "localhost";
    public static final boolean DEFAULT_AUTO_FLUSH = false;
    public static final String DEFAULT_TABLE_NAME = "securegraph";
    public static final int DEFAULT_MAX_STREAMING_PROPERTY_VALUE_TABLE_DATA_SIZE = 10 * 1024 * 1024;
    public static final String DEFAULT_HDFS_USER = "hadoop";
    public static final String DEFAULT_HDFS_ROOT_DIR = "";
    public static final String DEFAULT_DATA_DIR = "/accumuloGraph";
    public static final boolean DEFAULT_USE_SERVER_SIDE_ELEMENT_VISIBILITY_ROW_FILTER = true;

    public AccumuloGraphConfiguration(Map config) {
        super(config);
    }

    public Connector createConnector() throws AccumuloSecurityException, AccumuloException {
        LOGGER.info("Connecting to accumulo instance [{}] zookeeper servers [{}]", this.getAccumuloInstanceName(), this.getZookeeperServers());
        ZooKeeperInstance instance = new ZooKeeperInstance(this.getAccumuloInstanceName(), this.getZookeeperServers());
        return instance.getConnector(this.getAccumuloUsername(), this.getAuthenticationToken());
    }

    public FileSystem createFileSystem() throws URISyntaxException, IOException, InterruptedException {
        return FileSystem.get(getHdfsRootDir(), getHadoopConfiguration(), getHdfsUser());
    }

    private String getHdfsUser() {
        return getConfigString(HDFS_USER, DEFAULT_HDFS_USER);
    }

    private URI getHdfsRootDir() throws URISyntaxException {
        return new URI(getConfigString(HDFS_ROOT_DIR, DEFAULT_HDFS_ROOT_DIR));
    }

    private org.apache.hadoop.conf.Configuration getHadoopConfiguration() {
        org.apache.hadoop.conf.Configuration configuration = new org.apache.hadoop.conf.Configuration();
        for (Object entrySetObject : MapUtils.getAllWithPrefix(getConfig(), HDFS_CONFIG_PREFIX).entrySet()) {
            Map.Entry entrySet = (Map.Entry) entrySetObject;
            configuration.set("" + entrySet.getKey(), "" + entrySet.getValue());
        }
        return configuration;
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
        return ConfigurationUtils.createProvider(getConfig(), VALUE_SERIALIZER_PROP_PREFIX, DEFAULT_VALUE_SERIALIZER);
    }

    public boolean isAutoFlush() {
        return getConfigBoolean(AUTO_FLUSH, DEFAULT_AUTO_FLUSH);
    }

    public String getTableName() {
        return getConfigString(TABLE_NAME, DEFAULT_TABLE_NAME);
    }

    public long getMaxStreamingPropertyValueTableDataSize() {
        return getConfigLong(MAX_STREAMING_PROPERTY_VALUE_TABLE_DATA_SIZE, DEFAULT_MAX_STREAMING_PROPERTY_VALUE_TABLE_DATA_SIZE);
    }

    public String getDataDir() {
        return getConfigString(DATA_DIR, DEFAULT_DATA_DIR);
    }

    public boolean isUseServerSideElementVisibilityRowFilter() {
        return getConfigBoolean(USE_SERVER_SIDE_ELEMENT_VISIBILITY_ROW_FILTER, DEFAULT_USE_SERVER_SIDE_ELEMENT_VISIBILITY_ROW_FILTER);
    }
}
