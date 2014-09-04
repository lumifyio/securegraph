package org.securegraph.elasticsearch;

import org.junit.After;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.securegraph.Authorizations;
import org.securegraph.Graph;
import org.securegraph.elasticsearch.helpers.TestHelpers;
import org.securegraph.test.GraphTestBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(JUnit4.class)
public class ElasticSearchGraphTest extends GraphTestBase {
    private static final Logger LOGGER = LoggerFactory.getLogger(ElasticSearchGraphTest.class);

    @Override
    protected Graph createGraph() {
        return TestHelpers.createGraph();
    }

    @Override
    protected Authorizations createAuthorizations(String... auths) {
        return new ElasticSearchAuthorizations(auths);
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
}
