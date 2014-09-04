package org.securegraph.elasticsearch;

import org.securegraph.Authorizations;
import org.securegraph.SecureGraphException;
import org.securegraph.Visibility;
import org.securegraph.inmemory.security.ColumnVisibility;
import org.securegraph.inmemory.security.VisibilityEvaluator;
import org.securegraph.inmemory.security.VisibilityParseException;
import org.securegraph.util.ArrayUtils;

import java.io.Serializable;
import java.util.Arrays;

import static org.securegraph.util.Preconditions.checkNotNull;

public class ElasticSearchAuthorizations implements Authorizations, Serializable {
    private static final long serialVersionUID = 1L;
    private final String[] authorizations;

    public ElasticSearchAuthorizations(String... authorizations) {
        this.authorizations = authorizations;
    }

    @Override
    public String[] getAuthorizations() {
        return authorizations;
    }

    @Override
    public boolean equals(Authorizations authorizations) {
        return ArrayUtils.intersectsAll(getAuthorizations(), authorizations.getAuthorizations());
    }

    @Override
    public String toString() {
        return Arrays.toString(authorizations);
    }

    @Override
    public boolean canRead(Visibility visibility) {
        checkNotNull(visibility, "visibility is required");

        // this is just a shortcut so that we don't need to construct evaluators and visibility objects to check for an empty string.
        if (visibility.getVisibilityString().length() == 0) {
            return true;
        }

        VisibilityEvaluator visibilityEvaluator = new VisibilityEvaluator(new org.securegraph.inmemory.security.Authorizations(this.getAuthorizations()));
        ColumnVisibility columnVisibility = new ColumnVisibility(visibility.getVisibilityString());
        try {
            return visibilityEvaluator.evaluate(columnVisibility);
        } catch (VisibilityParseException e) {
            throw new SecureGraphException("could not evaluate visibility " + visibility.getVisibilityString(), e);
        }
    }
}
