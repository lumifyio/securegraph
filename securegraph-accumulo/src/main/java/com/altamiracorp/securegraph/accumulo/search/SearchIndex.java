package com.altamiracorp.securegraph.accumulo.search;

import com.altamiracorp.securegraph.Element;

public interface SearchIndex {
    void addElement(Element element);

    void removeElement(Element element);
}
