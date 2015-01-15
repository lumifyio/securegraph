package org.securegraph.elasticsearch.score;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilder;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;
import org.securegraph.*;
import org.securegraph.elasticsearch.BulkRequestWithCount;
import org.securegraph.elasticsearch.ElasticSearchSearchIndexBase;
import org.securegraph.elasticsearch.IndexInfo;
import org.securegraph.search.SearchIndex;

import java.io.IOException;

public class EdgeCountScoringStrategy extends ScoringStrategy {
    public static final String IN_EDGE_COUNT_FIELD_NAME = "__inEdgeCount";
    public static final String OUT_EDGE_COUNT_FIELD_NAME = "__outEdgeCount";

    private final EdgeCountScoringStrategyConfiguration config;

    public EdgeCountScoringStrategy(GraphConfiguration config) {
        super(config);
        this.config = new EdgeCountScoringStrategyConfiguration(config);
    }

    public EdgeCountScoringStrategyConfiguration getConfig() {
        return config;
    }

    @Override
    public void addElement(SearchIndex searchIndex, Graph graph, Element element, Authorizations authorizations) {
        if (!getConfig().isUpdateEdgeBoost()) {
            return;
        }
        if (!(element instanceof Edge)) {
            return;
        }

        Element vOut = ((Edge) element).getVertex(Direction.OUT, authorizations);
        if (vOut != null) {
            searchIndex.addElement(graph, vOut, authorizations);
        }

        Element vIn = ((Edge) element).getVertex(Direction.IN, authorizations);
        if (vIn != null) {
            searchIndex.addElement(graph, vIn, authorizations);
        }
    }

    @Override
    public int addElement(ElasticSearchSearchIndexBase searchIndex, Graph graph, BulkRequestWithCount bulkRequestWithCount, IndexInfo indexInfo, Element element, Authorizations authorizations) {
        int totalCount = 0;

        if (!getConfig().isUpdateEdgeBoost()) {
            return totalCount;
        }
        if (!(element instanceof Edge)) {
            return totalCount;
        }

        Element vOut = ((Edge) element).getVertex(Direction.OUT, authorizations);
        if (vOut != null) {
            searchIndex.addElementToBulkRequest(graph, bulkRequestWithCount.getBulkRequest(), indexInfo, vOut, authorizations);
            bulkRequestWithCount.incrementCount();
            totalCount++;
        }

        Element vIn = ((Edge) element).getVertex(Direction.IN, authorizations);
        if (vIn != null) {
            searchIndex.addElementToBulkRequest(graph, bulkRequestWithCount.getBulkRequest(), indexInfo, vIn, authorizations);
            bulkRequestWithCount.incrementCount();
            totalCount++;
        }

        return totalCount;
    }

    @Override
    public void addFieldsToElementType(XContentBuilder builder) throws IOException {
        builder
                .startObject(IN_EDGE_COUNT_FIELD_NAME).field("type", "integer").field("store", "true").endObject()
                .startObject(OUT_EDGE_COUNT_FIELD_NAME).field("type", "integer").field("store", "true").endObject()
        ;
    }

    @Override
    public QueryBuilder updateQuery(QueryBuilder query) {
        if (!getConfig().isUseEdgeBoost()) {
            return query;
        }

        ScoreFunctionBuilder scoreFunction = ScoreFunctionBuilders
                .scriptFunction("_score "
                        + " * sqrt(inEdgeMultiplier * (1 + doc['" + IN_EDGE_COUNT_FIELD_NAME + "'].value))"
                        + " * sqrt(outEdgeMultiplier * (1 + doc['" + OUT_EDGE_COUNT_FIELD_NAME + "'].value))"
                        , "groovy")
                .param("inEdgeMultiplier", getConfig().getInEdgeBoost())
                .param("outEdgeMultiplier", getConfig().getOutEdgeBoost());

        return QueryBuilders.functionScoreQuery(query, scoreFunction);
    }

    @Override
    public void addFieldsToVertexDocument(SearchIndex searchIndex, XContentBuilder jsonBuilder, Vertex vertex, Authorizations authorizations) throws IOException {
        if (!getConfig().isUpdateEdgeBoost()) {
            return;
        }

        int inEdgeCount = vertex.getEdgeCount(Direction.IN, authorizations);
        jsonBuilder.field(IN_EDGE_COUNT_FIELD_NAME, inEdgeCount);
        int outEdgeCount = vertex.getEdgeCount(Direction.OUT, authorizations);
        jsonBuilder.field(OUT_EDGE_COUNT_FIELD_NAME, outEdgeCount);
    }

    @Override
    public void addFieldsToEdgeDocument(SearchIndex searchIndex, XContentBuilder jsonBuilder, Edge edge, Authorizations authorizations) throws IOException {

    }
}
