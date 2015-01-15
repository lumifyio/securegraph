package org.securegraph.elasticsearch;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.securegraph.*;
import org.securegraph.elasticsearch.helpers.ElasticSearchSearchParentChildIndexTestHelpers;
import org.securegraph.elasticsearch.score.EdgeCountScoringStrategy;
import org.securegraph.elasticsearch.score.EdgeCountScoringStrategyConfiguration;
import org.securegraph.elasticsearch.score.ScoringStrategy;
import org.securegraph.inmemory.InMemoryAuthorizations;
import org.securegraph.test.GraphTestBase;
import org.securegraph.type.GeoPoint;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Map;

import static junit.framework.Assert.assertEquals;
import static junit.framework.TestCase.assertNotNull;

public class ElasticSearchSearchParentChildIndexTest extends GraphTestBase {
    @Override
    protected Graph createGraph() {
        return ElasticSearchSearchParentChildIndexTestHelpers.createGraph();
    }

    @Override
    protected Authorizations createAuthorizations(String... auths) {
        return new InMemoryAuthorizations(auths);
    }

    @Before
    @Override
    public void before() throws Exception {
        ElasticSearchSearchParentChildIndexTestHelpers.before();
        super.before();
    }

    @After
    public void after() throws Exception {
        super.after();
        ElasticSearchSearchParentChildIndexTestHelpers.after();
    }

    @Test
    public void testGeoPointLoadDefinition() {
        ElasticSearchParentChildSearchIndex searchIndex = (ElasticSearchParentChildSearchIndex) ((GraphBaseWithSearchIndex) graph).getSearchIndex();

        graph.prepareVertex("v1", VISIBILITY_A)
                .setProperty("location", new GeoPoint(38.9186, -77.2297, "Reston, VA"), VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        searchIndex.loadPropertyDefinitions();

        Map<String, PropertyDefinition> propertyDefinitions = searchIndex.getAllPropertyDefinitions();
        PropertyDefinition locationPropertyDef = propertyDefinitions.get("location");
        assertNotNull(locationPropertyDef);
        assertEquals(GeoPoint.class, locationPropertyDef.getDataType());
    }

    @Test
    public void testGetIndexRequests() throws IOException {
        Metadata prop1Metadata = new Metadata();
        prop1Metadata.add("metadata1", "metadata1Value", VISIBILITY_A);
        Vertex v1 = graph.prepareVertex("v1", VISIBILITY_A)
                .setProperty("prop1", "value1", prop1Metadata, VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        ElasticSearchParentChildSearchIndex searchIndex = (ElasticSearchParentChildSearchIndex) ((GraphBaseWithSearchIndex) graph).getSearchIndex();

        String indexName = searchIndex.getIndexName(v1);
        IndexInfo indexInfo = searchIndex.ensureIndexCreatedAndInitialized(indexName, searchIndex.getConfig().isStoreSourceData());
        assertNotNull(indexInfo);
    }

    @Override
    protected boolean disableUpdateEdgeCountInSearchIndex(Graph graph) {
        ElasticSearchParentChildSearchIndex searchIndex = (ElasticSearchParentChildSearchIndex) ((GraphBaseWithSearchIndex) graph).getSearchIndex();
        ElasticSearchSearchIndexConfiguration config = searchIndex.getConfig();
        ScoringStrategy scoringStrategy = config.getScoringStrategy();
        if (!(scoringStrategy instanceof EdgeCountScoringStrategy)) {
            return false;
        }

        EdgeCountScoringStrategyConfiguration edgeCountScoringStrategyConfig = ((EdgeCountScoringStrategy) scoringStrategy).getConfig();

        try {
            Field updateEdgeBoostField = edgeCountScoringStrategyConfig.getClass().getDeclaredField("updateEdgeBoost");
            updateEdgeBoostField.setAccessible(true);
            updateEdgeBoostField.set(edgeCountScoringStrategyConfig, false);
        } catch (Exception e) {
            throw new SecureGraphException("Failed to update 'updateEdgeBoost' field", e);
        }

        return true;
    }
}
