package org.securegraph.util;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public abstract class GroupingIterable<T> implements CloseableIterable<Group<T>> {
    private final Iterable<T> iterable;
    private boolean doneCalled;

    public GroupingIterable(Iterable<T> iterable) {
        this.iterable = iterable;
    }

    @Override
    public void close() {

    }

    private void callClose() {
        if (!doneCalled) {
            doneCalled = true;
            close();
        }
    }

    @Override
    public Iterator<Group<T>> iterator() {
        final Iterator<T> it = this.iterable.iterator();
        return new Iterator<Group<T>>() {
            private Group<T> next;
            private Group<T> current;
            private T itNext;

            @Override
            public boolean hasNext() {
                loadNext();
                if (next == null) {
                    callClose();
                }
                return next != null;
            }

            @Override
            public Group<T> next() {
                loadNext();
                this.current = this.next;
                this.next = null;
                return this.current;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }

            private void loadNext() {
                if (this.next != null) {
                    return;
                }

                String key = null;
                List<T> items = new ArrayList<>();
                if (itNext != null) {
                    key = getKey(itNext);
                    items.add(itNext);
                    itNext = null;
                }

                while (it.hasNext()) {
                    itNext = it.next();

                    String k = getKey(itNext);
                    if (key == null) {
                        key = k;
                        items.add(itNext);
                        itNext = null;
                    } else if (key.equals(k)) {
                        items.add(itNext);
                        itNext = null;
                    } else {
                        break;
                    }
                }

                if (key != null) {
                    this.next = new Group<>(key, items);
                }
            }
        };
    }

    protected abstract String getKey(T obj);
}
