package com.altamiracorp.securegraph.tools;

import com.altamiracorp.securegraph.Authorizations;
import com.altamiracorp.securegraph.Graph;
import com.altamiracorp.securegraph.GraphFactory;
import com.altamiracorp.securegraph.util.MapUtils;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;

public abstract class GraphToolBase {
    @Parameter(names = {"-c", "--config"}, description = "Configuration file name")
    private String configFileName = null;

    @Parameter(names = {"--configPrefix"}, description = "Prefix of graph related configuration parameters")
    private String configPrefix = null;

    @Parameter(names = {"-a", "--auth"}, description = "Comma separated string of Authorizations")
    private String authString = "";

    private Graph graph;

    protected void run(String[] args) throws Exception {
        new JCommander(this, args);

        if (configFileName == null) {
            throw new RuntimeException("config is required");
        }

        Map config = new Properties();
        InputStream in = new FileInputStream(configFileName);
        try {
            ((Properties) config).load(in);
        } finally {
            in.close();
        }
        if (configPrefix != null) {
            config = MapUtils.getAllWithPrefix(config, configPrefix);
        }
        graph = new GraphFactory().createGraph(config);
    }

    protected Authorizations getAuthorizations() {
        return new Authorizations(authString.split(","));
    }

    protected Graph getGraph() {
        return graph;
    }
}
