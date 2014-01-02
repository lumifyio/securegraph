package com.altamiracorp.securegraph.id;

import java.util.UUID;

public class UUIDIdGenerator implements IdGenerator {
    @Override
    public Object nextId() {
        return UUID.randomUUID().toString();
    }
}
