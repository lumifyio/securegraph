package com.altamiracorp.securegraph.elasticsearch;

import com.altamiracorp.securegraph.Authorizations;
import com.altamiracorp.securegraph.Graph;
import com.altamiracorp.securegraph.Vertex;
import com.altamiracorp.securegraph.elasticsearch.helpers.NestedTestHelpers;
import com.altamiracorp.securegraph.inmemory.InMemoryAuthorizations;
import com.altamiracorp.securegraph.inmemory.InMemoryGraph;
import com.altamiracorp.securegraph.property.PropertyValue;
import com.altamiracorp.securegraph.property.StreamingPropertyValue;
import com.altamiracorp.securegraph.test.GraphTestBase;
import com.altamiracorp.securegraph.test.util.LargeStringInputStream;
import org.apache.commons.io.IOUtils;
import org.json.JSONObject;
import org.junit.*;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static junit.framework.TestCase.assertNotNull;

public class ElasticSearchNestedSearchIndexTest extends GraphTestBase {
    @Override
    protected Graph createGraph() {
        return NestedTestHelpers.createGraph();
    }

    @Override
    protected Authorizations createAuthorizations(String... auths) {
        return new InMemoryAuthorizations(auths);
    }

    @Before
    @Override
    public void before() throws Exception {
        NestedTestHelpers.before();
        super.before();
    }

    @After
    public void after() throws Exception {
        super.after();
        NestedTestHelpers.after();
    }

    private ElasticSearchNestedSearchIndex getSearchIndex() {
        return (ElasticSearchNestedSearchIndex) ((InMemoryGraph) getGraph()).getSearchIndex();
    }

    @Test
    public void testCreateJsonForElement() throws IOException {
        Map<String, Object> prop1Metadata = new HashMap<String, Object>();
        prop1Metadata.put("metadata1", "metadata1Value");

        String expectedLargeValue = IOUtils.toString(new LargeStringInputStream(LARGE_PROPERTY_VALUE_SIZE));
        PropertyValue propSmall = new StreamingPropertyValue(new ByteArrayInputStream("value1".getBytes()), String.class);
        PropertyValue propLarge = new StreamingPropertyValue(new ByteArrayInputStream(expectedLargeValue.getBytes()), String.class);
        String largePropertyName = "propLarge/\\*!@#$%^&*()[]{}|";
        Vertex v1 = graph.prepareVertex("v1", VISIBILITY_A, AUTHORIZATIONS_A)
                .setProperty("propSmall", propSmall, VISIBILITY_A)
                .setProperty(largePropertyName, propLarge, VISIBILITY_A)
                .setProperty("prop1", "value1", prop1Metadata, VISIBILITY_A)
                .save();

        String jsonString = getSearchIndex().createJsonForElement(v1);
        JSONObject json = new JSONObject(jsonString);
        assertNotNull(json);
    }

    @Override
    @Ignore
    public void testGraphQueryHasWithSpacesAndFieldedQueryString() {
        System.out.println("Queries of the form name:value are not supported");
    }
}
