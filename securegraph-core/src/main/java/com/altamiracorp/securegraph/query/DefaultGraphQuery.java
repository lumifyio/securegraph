package com.altamiracorp.securegraph.query;

import com.altamiracorp.securegraph.*;

import java.util.Iterator;

public class DefaultGraphQuery extends GraphQueryBase {
    public DefaultGraphQuery(Graph graph, Authorizations authorizations) {
        super(graph, authorizations);
    }

    @Override
    public Iterable<Vertex> vertices() {
        return new DefaultGraphQueryIterable<Vertex>(ElementType.VERTEX);
    }

    @Override
    public Iterable<Edge> edges() {
        return new DefaultGraphQueryIterable<Edge>(ElementType.EDGE);
    }

    private class DefaultGraphQueryIterable<T extends Element> implements Iterable<T> {
        private final ElementType elementType;

        public DefaultGraphQueryIterable(ElementType elementType) {
            this.elementType = elementType;
        }

        @Override
        public Iterator<T> iterator() {
            final Iterator<T> it = getIterator();

            return new Iterator<T>() {
                public T next;
                public T current;
                public long count;

                @Override
                public boolean hasNext() {
                    loadNext();
                    return next != null;
                }

                @Override
                public T next() {
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

                    if (this.count >= parameters.getLimit()) {
                        return;
                    }

                    while (it.hasNext()) {
                        T elem = it.next();

                        boolean match = true;
                        for (HasContainer has : parameters.getHasContainers()) {
                            if (!has.isMatch(elem)) {
                                match = false;
                                break;
                            }
                        }
                        if (!match) {
                            continue;
                        }

                        this.count++;
                        if (this.count <= parameters.getSkip()) {
                            continue;
                        }

                        this.next = elem;
                        break;
                    }
                }
            };
        }

        @SuppressWarnings("unchecked")
        private Iterator<T> getIterator() {
            Iterator<T> it;
            switch (elementType) {
                case VERTEX:
                    it = (Iterator<T>) graph.getVertices(parameters.getAuthorizations()).iterator();
                    break;
                case EDGE:
                    it = (Iterator<T>) graph.getVertices(parameters.getAuthorizations()).iterator();
                    break;
                default:
                    throw new RuntimeException("Unexpected element type: " + elementType);
            }
            return it;
        }
    }
}
