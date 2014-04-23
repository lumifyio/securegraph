package org.securegraph;

import java.io.Serializable;

public interface Authorizations extends Serializable {
    boolean canRead(Visibility visibility);

    String[] getAuthorizations();
}
