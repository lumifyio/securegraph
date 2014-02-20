package com.altamiracorp.securegraph.id;

import com.altamiracorp.securegraph.SecureGraphException;

import java.util.LinkedList;
import java.util.Queue;

public class QueueIdGenerator implements IdGenerator {
    private Queue<Object> ids = new LinkedList<Object>();

    @Override
    public Object nextId() {
        synchronized (ids) {
            if (ids.size() == 0) {
                throw new SecureGraphException("No ids in the queue to give out");
            }
            return ids.remove();
        }
    }

    public void push(Object id) {
        ids.add(id);
    }
}
