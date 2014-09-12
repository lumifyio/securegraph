package org.securegraph.elasticsearch;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.securegraph.*;
import org.securegraph.elasticsearch.helpers.TestHelpers;
import org.securegraph.inmemory.InMemoryAuthorizations;
import org.securegraph.test.GraphTestBase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static junit.framework.TestCase.assertNotNull;

public class ElasticSearchSearchParentChildIndexTest extends GraphTestBase {
    @Override
    protected Graph createGraph() {
        return TestHelpers.createGraph();
    }

    @Override
    protected Authorizations createAuthorizations(String... auths) {
        return new InMemoryAuthorizations(auths);
    }

    @Before
    @Override
    public void before() throws Exception {
        TestHelpers.before();
        super.before();
    }

    @After
    public void after() throws Exception {
        super.after();
        TestHelpers.after();
    }

    @Test
    public void testGetIndexRequests() throws IOException {
        Map<String, Object> prop1Metadata = new HashMap<String, Object>();
        prop1Metadata.put("metadata1", "metadata1Value");
        Vertex v1 = graph.prepareVertex("v1", VISIBILITY_A)
                .setProperty("prop1", "value1", prop1Metadata, VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);
        graph.flush();

        ElasticSearchParentChildSearchIndex searchIndex = (ElasticSearchParentChildSearchIndex) ((GraphBaseWithSearchIndex) graph).getSearchIndex();

        String indexName = searchIndex.getIndexName(v1);
        IndexInfo indexInfo = searchIndex.ensureIndexCreatedAndInitialized(indexName, searchIndex.isStoreSourceData());

        String parentDocumentJson = searchIndex.getParentDocumentIndexRequest(v1, AUTHORIZATIONS_A_AND_B).source().toUtf8();
        assertNotNull(parentDocumentJson);
        for (Property property : v1.getProperties()) {
            String propertyJson = searchIndex.getPropertyDocumentIndexRequest(v1, property).source().toUtf8();
            assertNotNull(propertyJson);
        }
    }
}
