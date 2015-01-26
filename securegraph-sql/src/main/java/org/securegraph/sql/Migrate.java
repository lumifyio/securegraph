package org.securegraph.sql;

import liquibase.Liquibase;
import liquibase.database.DatabaseConnection;
import liquibase.exception.LiquibaseException;
import liquibase.resource.ClassLoaderResourceAccessor;

public class Migrate {
    public void migrate(DatabaseConnection conn) {
        try {
            String changeLogFile = "org/securegraph/sql/databaseChangeLog.xml";
            Liquibase liquibase = new Liquibase(changeLogFile, new ClassLoaderResourceAccessor(), conn);
            liquibase.update("");
        } catch (LiquibaseException ex) {
            throw new SecurityException("Could not migrate database", ex);
        }
    }
}
