package org.securegraph.sql;

import liquibase.database.DatabaseConnection;
import liquibase.database.jvm.JdbcConnection;
import org.securegraph.GraphConfiguration;
import org.securegraph.SecureGraphException;
import org.securegraph.sql.serializer.JavaValueSerializer;
import org.securegraph.sql.serializer.ValueSerializer;
import org.securegraph.util.ConfigurationUtils;
import org.sql2o.GenericDatasource;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;

public class SqlGraphConfiguration extends GraphConfiguration {
    public static final String JDBC_URL = "jdbcUrl";
    public static final String JDBC_CLASS_NAME = "jdbcClassName";
    public static final String JDBC_CONNECTION_PROPERTY_PREFIX = "jdbcConnectionProperty.";
    public static final String VALUE_SERIALIZER = "serializer";

    public static final String DEFAULT_VALUE_SERIALIZER = JavaValueSerializer.class.getName();

    private Properties connectionProperties;

    public SqlGraphConfiguration(Map config) {
        super(config);
    }

    public DatabaseConnection createMigrationConnection() {
        return new JdbcConnection(createJdbcConnection());
    }

    public Connection createJdbcConnection() {
        String jdbcClassName = getJdbcClassName();
        if (jdbcClassName == null || jdbcClassName.trim().length() == 0) {
            throw new SecureGraphException(JDBC_CLASS_NAME + " is required");
        }

        String jdbcUrl = getJdbcUrl();
        if (jdbcUrl == null || jdbcUrl.trim().length() == 0) {
            throw new SecureGraphException(JDBC_URL + " is required");
        }

        try {
            Class.forName(jdbcClassName);
        } catch (ClassNotFoundException ex) {
            throw new SecureGraphException("Could not load jdbc class: " + jdbcClassName, ex);
        }
        Properties connectionProperties = getJdbcProperties();
        try {
            return DriverManager.getConnection(jdbcUrl, connectionProperties);
        } catch (SQLException ex) {
            throw new SecureGraphException("Could not create connection: " + jdbcUrl, ex);
        }
    }

    private Properties getJdbcProperties() {
        if (connectionProperties == null) {
            connectionProperties = new Properties();
            for (Object configEntryObj : getConfig().entrySet()) {
                Map.Entry configEntry = (Map.Entry) configEntryObj;
                String key = (String) configEntry.getKey();
                if (key.startsWith(JDBC_CONNECTION_PROPERTY_PREFIX)) {
                    connectionProperties.put(key.substring(JDBC_CONNECTION_PROPERTY_PREFIX.length()), configEntry.getValue());
                }
            }
        }
        return connectionProperties;
    }

    private String getJdbcUrl() {
        return getString(JDBC_URL, null);
    }

    public String getJdbcClassName() {
        return getString(JDBC_CLASS_NAME, null);
    }

    public DataSource getDataSource() {
        return new GenericDatasource(getJdbcUrl(), getJdbcProperties());
    }

    public ValueSerializer createValueSerializer() throws SecureGraphException {
        return ConfigurationUtils.createProvider(this, VALUE_SERIALIZER, DEFAULT_VALUE_SERIALIZER);
    }
}
