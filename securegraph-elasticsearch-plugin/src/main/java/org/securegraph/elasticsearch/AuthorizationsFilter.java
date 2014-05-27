package org.securegraph.elasticsearch;

import org.apache.lucene.index.*;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.Filter;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.OpenBitSet;
import org.securegraph.inmemory.security.Authorizations;
import org.securegraph.inmemory.security.ColumnVisibility;
import org.securegraph.inmemory.security.VisibilityEvaluator;
import org.securegraph.inmemory.security.VisibilityParseException;

import java.io.IOException;

public class AuthorizationsFilter extends Filter {
    public static String VISIBILITY_FIELD_NAME = "__visibility";
    private final Authorizations authorizations;

    public AuthorizationsFilter(Authorizations authorizations) {
        this.authorizations = authorizations;
    }

    @Override
    public DocIdSet getDocIdSet(AtomicReaderContext context, Bits acceptDocs) throws IOException {
        AtomicReader reader = context.reader();
        Fields fields = reader.fields();
        Terms terms = fields.terms(VISIBILITY_FIELD_NAME);
        if (terms == null) {
            if (acceptDocs instanceof DocIdSet) {
                return (DocIdSet) acceptDocs;
            } else {
                return wrap(acceptDocs);
            }
        }

        OpenBitSet bitSet = new OpenBitSet(reader.maxDoc());
        TermsEnum iterator = terms.iterator(null);
        BytesRef bytesRef;
        VisibilityEvaluator visibilityEvaluator = new VisibilityEvaluator(authorizations);
        while ((bytesRef = iterator.next()) != null) {
            if (isVisible(visibilityEvaluator, bytesRef)) {
                makeVisible(terms, iterator, bytesRef, bitSet, acceptDocs);
            }
        }
        return bitSet;
    }

    private void makeVisible(Terms terms, TermsEnum iterator, BytesRef bytesRef, OpenBitSet bitSet, Bits liveDocs) throws IOException {
        DocsEnum docsEnum = iterator.docs(liveDocs, null);
        int doc;
        while ((doc = docsEnum.nextDoc()) != DocsEnum.NO_MORE_DOCS) {
            bitSet.set(doc);
        }
    }

    private boolean isVisible(VisibilityEvaluator visibilityEvaluator, BytesRef bytesRef) throws IOException {
        ColumnVisibility visibility = new ColumnVisibility(trim(bytesRef));
        try {
            return visibilityEvaluator.evaluate(visibility);
        } catch (VisibilityParseException e) {
            throw new IOException(e);
        }
    }

    private byte[] trim(BytesRef bytesRef) {
        byte[] buf = new byte[bytesRef.length];
        System.arraycopy(bytesRef.bytes, bytesRef.offset, buf, 0, bytesRef.length);
        return buf;
    }

    private DocIdSet wrap(Bits acceptDocs) throws IOException {
        throw new IOException("not implemented");
    }
}
