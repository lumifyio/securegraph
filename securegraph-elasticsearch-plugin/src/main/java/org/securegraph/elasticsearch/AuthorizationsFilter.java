package org.securegraph.elasticsearch;

import org.apache.lucene.index.*;
import org.apache.lucene.search.BitsFilteredDocIdSet;
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
            return null;
        } else {
            OpenBitSet bitSet = new OpenBitSet(reader.maxDoc());
            TermsEnum iterator = terms.iterator(null);
            BytesRef bytesRef;
            VisibilityEvaluator visibilityEvaluator = new VisibilityEvaluator(authorizations);
            while ((bytesRef = iterator.next()) != null) {
                makeVisible(iterator, bitSet, acceptDocs, isVisible(visibilityEvaluator, bytesRef));
            }
            return BitsFilteredDocIdSet.wrap(bitSet, acceptDocs);
        }
    }

    private void makeVisible(TermsEnum iterator, OpenBitSet bitSet, Bits liveDocs, boolean visible) throws IOException {
        DocsEnum docsEnum = iterator.docs(liveDocs, null);
        int doc;
        while ((doc = docsEnum.nextDoc()) != DocsEnum.NO_MORE_DOCS) {
            if (visible) {
                bitSet.set(doc);
            } else {
                bitSet.clear(doc);
            }
        }
    }

    private boolean isVisible(VisibilityEvaluator visibilityEvaluator, BytesRef bytesRef) throws IOException {
        byte[] expression = trim(bytesRef);
        if (expression.length == 0) {
            return true;
        }
        ColumnVisibility visibility = new ColumnVisibility(expression);
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
}
