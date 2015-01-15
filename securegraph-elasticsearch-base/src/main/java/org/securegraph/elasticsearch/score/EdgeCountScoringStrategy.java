package org.securegraph.elasticsearch.score;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.securegraph.*;
import org.securegraph.elasticsearch.BulkRequestWithCount;
import org.securegraph.elasticsearch.ElasticSearchSearchIndexBase;
import org.securegraph.elasticsearch.IndexInfo;
import org.securegraph.search.SearchIndex;

import java.io.IOException;

public class EdgeCountScoringStrategy extends ScoringStrategy {
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
        if (getConfig().isUpdateEdgeBoost() && element instanceof Edge) {
            Element vOut = ((Edge) element).getVertex(Direction.OUT, authorizations);
            if (vOut != null) {
                searchIndex.addElement(graph, vOut, authorizations);
            }
            Element vIn = ((Edge) element).getVertex(Direction.IN, authorizations);
            if (vIn != null) {
                searchIndex.addElement(graph, vIn, authorizations);
            }
        }
    }

    @Override
    public int addElement(ElasticSearchSearchIndexBase searchIndex, Graph graph, BulkRequestWithCount bulkRequestWithCount, IndexInfo indexInfo, Element element, Authorizations authorizations) {
        int totalCount = 0;

        if (!getConfig().isUpdateEdgeBoost() || !(element instanceof Edge)) {
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
    public void addFieldsToVertexDocument(SearchIndex searchIndex, XContentBuilder jsonBuilder, Vertex vertex, Authorizations authorizations) throws IOException {
        if (getConfig().isUpdateEdgeBoost()) {
            int inEdgeCount = vertex.getEdgeCount(Direction.IN, authorizations);
            jsonBuilder.field(ElasticSearchSearchIndexBase.IN_EDGE_COUNT_FIELD_NAME, inEdgeCount);
            int outEdgeCount = vertex.getEdgeCount(Direction.OUT, authorizations);
            jsonBuilder.field(ElasticSearchSearchIndexBase.OUT_EDGE_COUNT_FIELD_NAME, outEdgeCount);
        }
    }

    @Override
    public void addFieldsToEdgeDocument(SearchIndex searchIndex, XContentBuilder jsonBuilder, Edge edge, Authorizations authorizations) throws IOException {

    }
}
