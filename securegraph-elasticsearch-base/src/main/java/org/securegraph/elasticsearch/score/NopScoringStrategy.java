package org.securegraph.elasticsearch.score;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.securegraph.*;
import org.securegraph.elasticsearch.BulkRequestWithCount;
import org.securegraph.elasticsearch.ElasticSearchSearchIndexBase;
import org.securegraph.elasticsearch.IndexInfo;
import org.securegraph.search.SearchIndex;

import java.io.IOException;

public class NopScoringStrategy extends ScoringStrategy {
    public NopScoringStrategy(GraphConfiguration graphConfiguration) {
        super(graphConfiguration);
    }

    @Override
    public void addElement(SearchIndex searchIndex, Graph graph, Element element, Authorizations authorizations) {

    }

    @Override
    public void addFieldsToVertexDocument(SearchIndex searchIndex, XContentBuilder jsonBuilder, Vertex vertex, Authorizations authorizations) throws IOException {

    }

    @Override
    public void addFieldsToEdgeDocument(SearchIndex searchIndex, XContentBuilder jsonBuilder, Edge edge, Authorizations authorizations) throws IOException {

    }

    @Override
    public int addElement(ElasticSearchSearchIndexBase searchIndex, Graph graph, BulkRequestWithCount bulkRequestWithCount, IndexInfo indexInfo, Element element, Authorizations authorizations) {
        return 0;
    }
}
