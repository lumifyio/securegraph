package org.securegraph.sql;

import org.securegraph.inmemory.InMemoryAuthorizations;

public class SqlAuthorizations extends InMemoryAuthorizations {
    public SqlAuthorizations(String[] auths) {
        super(auths);
    }
}
