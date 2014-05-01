package org.securegraph.query;

public class FacetedTerm {
    private final String term;
    private final int count;

    public FacetedTerm(String term, int count) {
        this.term = term;
        this.count = count;
    }

    public String getTerm() {
        return term;
    }

    public int getCount() {
        return count;
    }
}
