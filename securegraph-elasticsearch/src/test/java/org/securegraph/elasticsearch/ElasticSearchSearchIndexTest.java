package org.securegraph.elasticsearch;

import org.apache.commons.io.IOUtils;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.securegraph.Authorizations;
import org.securegraph.Graph;
import org.securegraph.Metadata;
import org.securegraph.Vertex;
import org.securegraph.elasticsearch.helpers.ElasticSearchSearchIndexTestHelpers;
import org.securegraph.inmemory.InMemoryAuthorizations;
import org.securegraph.inmemory.InMemoryGraph;
import org.securegraph.property.PropertyValue;
import org.securegraph.property.StreamingPropertyValue;
import org.securegraph.test.GraphTestBase;
import org.securegraph.test.util.LargeStringInputStream;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import static junit.framework.TestCase.assertNotNull;

public class ElasticSearchSearchIndexTest extends GraphTestBase {
    @Override
    protected Graph createGraph() {
        return ElasticSearchSearchIndexTestHelpers.createGraph();
    }

    @Override
    protected Authorizations createAuthorizations(String... auths) {
        return new InMemoryAuthorizations(auths);
    }

    @Before
    @Override
    public void before() throws Exception {
        ElasticSearchSearchIndexTestHelpers.before();
        super.before();
    }

    @After
    public void after() throws Exception {
        super.after();
        ElasticSearchSearchIndexTestHelpers.after();
    }

    private ElasticSearchSearchIndex getSearchIndex() {
        return (ElasticSearchSearchIndex) ((InMemoryGraph) getGraph()).getSearchIndex();
    }

    @Test
    public void testCreateJsonForElement() throws IOException {
        Metadata prop1Metadata = new Metadata();
        prop1Metadata.add("metadata1", "metadata1Value", VISIBILITY_A);

        String expectedLargeValue = IOUtils.toString(new LargeStringInputStream(LARGE_PROPERTY_VALUE_SIZE));
        PropertyValue propSmall = new StreamingPropertyValue(new ByteArrayInputStream("value1".getBytes()), String.class);
        PropertyValue propLarge = new StreamingPropertyValue(new ByteArrayInputStream(expectedLargeValue.getBytes()), String.class);
        String largePropertyName = "propLarge/\\*!@#$%^&*()[]{}|";
        Vertex v1 = graph.prepareVertex("v1", VISIBILITY_A)
                .setProperty("propSmall", propSmall, VISIBILITY_A)
                .setProperty(largePropertyName, propLarge, VISIBILITY_A)
                .setProperty("prop1", "value1", prop1Metadata, VISIBILITY_A)
                .save(AUTHORIZATIONS_A_AND_B);

        String jsonString = getSearchIndex().createJsonForElement(graph, v1, true, AUTHORIZATIONS_A_AND_B);
        JSONObject json = new JSONObject(jsonString);
        assertNotNull(json);

        getSearchIndex().loadPropertyDefinitions();
    }
}
