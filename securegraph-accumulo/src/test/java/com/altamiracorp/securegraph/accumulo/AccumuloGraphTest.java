package com.altamiracorp.securegraph.accumulo;

import com.altamiracorp.securegraph.Graph;
import com.altamiracorp.securegraph.test.GraphTestBase;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class AccumuloGraphTest extends GraphTestBase {
    @Override
    protected Graph createGraph() {
        return new AccumuloGraph();
    }
}
