package org.securegraph.elasticsearch.score;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.securegraph.*;
import org.securegraph.elasticsearch.BulkRequestWithCount;
import org.securegraph.elasticsearch.ElasticSearchSearchIndexBase;
import org.securegraph.elasticsearch.IndexInfo;
import org.securegraph.search.SearchIndex;

import java.io.IOException;

public abstract class ScoringStrategy {
    private final GraphConfiguration graphConfiguration;

    protected ScoringStrategy(GraphConfiguration graphConfiguration) {
        this.graphConfiguration = graphConfiguration;
    }

    protected GraphConfiguration getGraphConfiguration() {
        return graphConfiguration;
    }

    public abstract void addElement(SearchIndex searchIndex, Graph graph, Element element, Authorizations authorizations);

    public abstract void addFieldsToVertexDocument(SearchIndex searchIndex, XContentBuilder jsonBuilder, Vertex vertex, Authorizations authorizations) throws IOException;

    public abstract void addFieldsToEdgeDocument(SearchIndex searchIndex, XContentBuilder jsonBuilder, Edge edge, Authorizations authorizations) throws IOException;

    public abstract int addElement(ElasticSearchSearchIndexBase searchIndex, Graph graph, BulkRequestWithCount bulkRequestWithCount, IndexInfo indexInfo, Element element, Authorizations authorizations);

    public abstract void addFieldsToElementType(XContentBuilder builder) throws IOException;

    public abstract QueryBuilder updateQuery(QueryBuilder query);
}
