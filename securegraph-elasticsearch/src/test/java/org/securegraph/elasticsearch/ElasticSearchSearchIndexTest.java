package org.securegraph.elasticsearch;

import org.apache.commons.io.IOUtils;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.securegraph.Authorizations;
import org.securegraph.Graph;
import org.securegraph.Vertex;
import org.securegraph.elasticsearch.helpers.TestHelpers;
import org.securegraph.inmemory.InMemoryAuthorizations;
import org.securegraph.inmemory.InMemoryGraph;
import org.securegraph.property.PropertyValue;
import org.securegraph.property.StreamingPropertyValue;
import org.securegraph.test.GraphTestBase;
import org.securegraph.test.util.LargeStringInputStream;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static junit.framework.TestCase.assertNotNull;

public class ElasticSearchSearchIndexTest extends GraphTestBase {
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

    private ElasticSearchSearchIndex getSearchIndex() {
        return (ElasticSearchSearchIndex) ((InMemoryGraph) getGraph()).getSearchIndex();
    }

    @Test
    public void testCreateJsonForElement() throws IOException {
        Map<String, Object> prop1Metadata = new HashMap<String, Object>();
        prop1Metadata.put("metadata1", "metadata1Value");

        String expectedLargeValue = IOUtils.toString(new LargeStringInputStream(LARGE_PROPERTY_VALUE_SIZE));
        PropertyValue propSmall = new StreamingPropertyValue(new ByteArrayInputStream("value1".getBytes()), String.class);
        PropertyValue propLarge = new StreamingPropertyValue(new ByteArrayInputStream(expectedLargeValue.getBytes()), String.class);
        String largePropertyName = "propLarge/\\*!@#$%^&*()[]{}|";
        Vertex v1 = graph.prepareVertex("v1", VISIBILITY_A)
                .setProperty("propSmall", propSmall, VISIBILITY_A)
                .setProperty(largePropertyName, propLarge, VISIBILITY_A)
                .setProperty("prop1", "value1", prop1Metadata, VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);

        String jsonString = getSearchIndex().createJsonForElement(v1, AUTHORIZATIONS_A_AND_B);
        JSONObject json = new JSONObject(jsonString);
        assertNotNull(json);
    }
}
