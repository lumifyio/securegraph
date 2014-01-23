package com.altamiracorp.securegraph.inmemory;

import com.altamiracorp.securegraph.*;
import com.altamiracorp.securegraph.id.UUIDIdGenerator;
import com.altamiracorp.securegraph.search.DefaultSearchIndex;
import com.altamiracorp.securegraph.test.GraphTestBase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static com.altamiracorp.securegraph.test.util.IterableUtils.count;
import static org.junit.Assert.assertEquals;

@RunWith(JUnit4.class)
public class InMemoryGraphTest extends GraphTestBase {
    @Override
    protected Graph createGraph() {
        Map config = new HashMap();
        config.put("", InMemoryGraph.class.getName());
        config.put(GraphConfiguration.IDGENERATOR_PROP_PREFIX, UUIDIdGenerator.class.getName());
        config.put(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX, DefaultSearchIndex.class.getName());
        return new GraphFactory().createGraph(config);
    }

    @Override
    public InMemoryGraph getGraph() {
        return (InMemoryGraph) super.getGraph();
    }

    @Override
    protected Authorizations createAuthorizations(String... auths) {
        return new InMemoryAuthorizations(auths);
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
