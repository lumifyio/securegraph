package com.altamiracorp.securegraph.inmemory;

import com.altamiracorp.securegraph.Graph;
import com.altamiracorp.securegraph.GraphConfiguration;
import com.altamiracorp.securegraph.id.UUIDIdGenerator;
import com.altamiracorp.securegraph.search.DefaultSearchIndex;
import com.altamiracorp.securegraph.test.GraphTestBase;
import org.junit.After;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.HashMap;
import java.util.Map;

@RunWith(JUnit4.class)
public class InMemoryGraphTest extends GraphTestBase {
    @Override
    protected Graph createGraph() {
        Map config = new HashMap();
        return new InMemoryGraph(new GraphConfiguration(config), new UUIDIdGenerator(config), new DefaultSearchIndex(config));
    }

    @Before
    @Override
    public void before() throws Exception {
        super.before();
    }

    @After
    public void after() throws Exception {
        super.after();
    }
}
