package org.securegraph.elasticsearch;

import org.apache.lucene.search.Filter;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.FilterParser;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.query.QueryParsingException;
import org.securegraph.inmemory.security.Authorizations;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class AuthorizationsFilterParser implements FilterParser {
    private static final String NAME = "authorizations";

    @Override
    public String[] names() {
        return new String[]{NAME};
    }

    @Override
    public Filter parse(QueryParseContext parseContext) throws IOException, QueryParsingException {
        XContentParser parser = parseContext.parser();

        XContentParser.Token token = parser.currentToken();
        if (token != XContentParser.Token.START_ARRAY) {
            throw new QueryParsingException(parseContext.index(), "authorizations must be an array.");
        }

        List<String> authorizations = new ArrayList<String>();
        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
            if (token != XContentParser.Token.VALUE_STRING) {
                throw new QueryParsingException(parseContext.index(), "authorizations must be an array of strings.");
            }

            String authorization = parser.text();
            authorizations.add(authorization);
        }

        return new AuthorizationsFilter(new Authorizations(authorizations.toArray(new String[authorizations.size()])));
    }
}
