package org.securegraph.elasticsearch;

import org.junit.After;
import org.junit.Before;
import org.securegraph.Authorizations;
import org.securegraph.Graph;
import org.securegraph.elasticsearch.helpers.TestHelpers;
import org.securegraph.inmemory.InMemoryAuthorizations;
import org.securegraph.test.GraphTestBase;

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
}
