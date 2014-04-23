package org.securegraph.id;

import java.util.Map;
import java.util.UUID;

public class UUIDIdGenerator implements IdGenerator {
    public UUIDIdGenerator(Map configuration) {

    }

    @Override
    public Object nextId() {
        return UUID.randomUUID().toString().replaceAll("-", "");
    }
}
