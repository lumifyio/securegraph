package com.altamiracorp.securegraph.accumulo;

import com.altamiracorp.securegraph.Authorizations;
import com.altamiracorp.securegraph.Graph;
import com.altamiracorp.securegraph.SecureGraphException;
import com.altamiracorp.securegraph.Vertex;
import com.altamiracorp.securegraph.accumulo.helpers.TestHelpers;
import com.altamiracorp.securegraph.test.GraphTestBase;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.RowIterator;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.fail;
import static org.junit.Assert.assertNotEquals;

@RunWith(JUnit4.class)
public class AccumuloGraphTest extends GraphTestBase {
    @Override
    protected Graph createGraph() throws AccumuloSecurityException, AccumuloException, SecureGraphException, InterruptedException, IOException, URISyntaxException {
        return TestHelpers.createGraph();
    }

    @Override
    protected Authorizations createAuthorizations(String... auths) {
        return new AccumuloAuthorizations(auths);
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
    public void testStoringEmptyMetadata() {
        Vertex v1 = graph.addVertex("v1", VISIBILITY_EMPTY, AUTHORIZATIONS_EMPTY);
        Map<String, Object> metadata = new HashMap<String, Object>();
        v1.addPropertyValue("prop1", "prop1", "val1", metadata, VISIBILITY_EMPTY);

        Vertex v2 = graph.addVertex("v2", VISIBILITY_EMPTY, AUTHORIZATIONS_EMPTY);
        metadata = new HashMap<String, Object>();
        metadata.put("meta1", "metavalue1");
        v2.addPropertyValue("prop1", "prop1", "val1", metadata, VISIBILITY_EMPTY);

        v1 = graph.getVertex("v1", AUTHORIZATIONS_EMPTY);
        assertEquals(0, v1.getProperty("prop1", "prop1").getMetadata().size());

        v2 = graph.getVertex("v2", AUTHORIZATIONS_EMPTY);
        metadata = v2.getProperty("prop1", "prop1").getMetadata();
        assertEquals(1, metadata.size());
        assertEquals("metavalue1", metadata.get("meta1"));

        AccumuloGraph accumuloGraph = (AccumuloGraph) graph;
        Scanner vertexScanner = accumuloGraph.createVertexScanner(AUTHORIZATIONS_EMPTY);
        vertexScanner.setRange(new Range("V", "W"));
        RowIterator rows = new RowIterator(vertexScanner.iterator());
        while (rows.hasNext()) {
            Iterator<Map.Entry<Key, Value>> row = rows.next();
            while (row.hasNext()) {
                Map.Entry<Key, Value> col = row.next();
                if (col.getKey().getColumnFamily().equals(AccumuloElement.CF_PROPERTY_METADATA)) {
                    if (col.getKey().getRow().toString().equals("Vv1")) {
                        assertEquals("", col.getValue().toString());
                    } else if (col.getKey().getRow().toString().equals("Vv2")) {
                        assertNotEquals("", col.getValue().toString());
                    } else {
                        fail("invalid vertex");
                    }
                }
            }
        }
    }
}
