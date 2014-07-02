package org.securegraph.examples;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.server.nio.SelectChannelConnector;
import org.eclipse.jetty.webapp.WebAppContext;
import org.json.JSONArray;
import org.json.JSONObject;
import org.securegraph.*;
import org.securegraph.accumulo.AccumuloAuthorizations;
import org.securegraph.accumulo.AccumuloGraph;
import org.securegraph.examples.dataset.BabyNamesDataset;
import org.securegraph.examples.dataset.Dataset;
import org.securegraph.examples.dataset.GeoNamesDataset;
import org.securegraph.examples.dataset.ImdbDataset;
import org.securegraph.util.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.Servlet;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;

import static org.securegraph.util.IterableUtils.count;

public abstract class ExampleBase {
    protected static final Logger LOGGER = LoggerFactory.getLogger(ExampleBase.class);
    private static final String VISIBILITIES[] = new String[]{"a", "b", "c", "d"};
    private static final int VERTICES_TO_CREATE = 3000;
    private Dataset dataset;

    @Parameter(names = "-port", description = "Port to run server on")
    private int port = 7777;

    @Parameter(names = "-clear", description = "Clear before running")
    private boolean clear = false;

    @Parameter(names = "-dataset", description = "Name of the dataset")
    private String datasetName = null;

    private Server server;
    private Graph graph;

    protected void run(String[] args) throws Exception {
        new JCommander(this, args);

        this.server = runJetty(port);
        this.graph = openGraph(getGraphConfig());

        if (datasetName == null) {
            throw new RuntimeException("must specify a dataset");
        } else if (datasetName.equals("GeoNamesDataset")) {
            dataset = new GeoNamesDataset();
        } else if (datasetName.equals("BabyNamesDataset")) {
            dataset = new BabyNamesDataset();
        } else if (datasetName.equals("ImdbDataset")) {
            dataset = new ImdbDataset();
        } else {
            throw new RuntimeException("Unknown dataset name");
        }

        clearGraph(this.graph);
        populateData();
    }

    protected void clearGraph(Graph graph) {
        if (!clear) {
            int count = count(graph.getVertices(createAuthorizations(VISIBILITIES)));
            if (count >= VERTICES_TO_CREATE) {
                LOGGER.debug("skipping clear graph. data already exists. count: " + count);
                return;
            }
        }
        LOGGER.debug("clearing vertices");
        graph.clearData();
    }

    protected void populateData() throws IOException {
        if (count(getGraph().getVertices(createAuthorizations(VISIBILITIES))) >= VERTICES_TO_CREATE) {
            LOGGER.debug("skipping create data. data already exists");
            return;
        }

        addAuthorizations();
        populateVertices();
    }

    private void populateVertices() throws IOException {
        dataset.load(getGraph(), VERTICES_TO_CREATE, VISIBILITIES, createAuthorizations());
    }

    private void addAuthorizations() {
        for (String v : VISIBILITIES) {
            addAuthorizationToUser(v);
        }
    }

    protected void stop() throws Exception {
        stopJetty();
        stopGraph();
    }

    protected static Authorizations createAuthorizations(String... auths) {
        if (auths.length == 1 && auths[0].length() == 0) {
            return new AccumuloAuthorizations();
        }
        return new AccumuloAuthorizations(auths);
    }

    protected void addAuthorizationToUser(String visibilityString) {
        LOGGER.debug("adding auth " + visibilityString);
        if (getGraph() instanceof AccumuloGraph) {
            try {
                org.apache.accumulo.core.client.Connector connector = ((AccumuloGraph) getGraph()).getConnector();
                String principal = ((AccumuloGraph) getGraph()).getConnector().whoami();
                org.apache.accumulo.core.security.Authorizations authorizations = connector.securityOperations().getUserAuthorizations(principal);
                if (authorizations.contains(visibilityString)) {
                    return;
                }
                String[] newAuthorizations = new String[authorizations.getAuthorizations().size() + 1];
                int i;
                for (i = 0; i < authorizations.getAuthorizations().size(); i++) {
                    newAuthorizations[i] = new String(authorizations.getAuthorizations().get(i));
                }
                newAuthorizations[i] = visibilityString;
                connector.securityOperations().changeUserAuthorizations(principal, new org.apache.accumulo.core.security.Authorizations(newAuthorizations));
            } catch (Exception ex) {
                throw new RuntimeException("Could not add auths", ex);
            }
        } else {
            throw new RuntimeException("Unhandled graph type to add authorizations: " + getGraph().getClass().getName());
        }
    }

    protected Graph openGraph(Map graphConfig) throws IOException {
        return new GraphFactory().createGraph(graphConfig);
    }

    private Map getGraphConfig() throws IOException {
        Properties config = new Properties();
        InputStream in = new FileInputStream("config.properties");
        try {
            config.load(in);
        } finally {
            in.close();
        }
        return MapUtils.getAllWithPrefix(config, "graph");
    }

    protected void stopGraph() {
        this.graph.shutdown();
    }

    protected Server runJetty(int httpPort) throws Exception {
        SelectChannelConnector httpConnector = new SelectChannelConnector();
        httpConnector.setPort(httpPort);

        WebAppContext webAppContext = new WebAppContext();
        webAppContext.setContextPath("/");
        webAppContext.addServlet(getServletClass(), "/*");
        webAppContext.setWar("./src/main/webapp/");

        ContextHandlerCollection contexts = new ContextHandlerCollection();
        contexts.setHandlers(new Handler[]{webAppContext});

        Server server = new Server();
        server.setConnectors(new Connector[]{httpConnector});
        server.setHandler(contexts);

        server.start();

        LOGGER.info("Listening http://localhost:" + httpPort);

        return server;
    }

    protected abstract Class<? extends Servlet> getServletClass();

    protected void stopJetty() throws Exception {
        server.stop();
    }

    public Graph getGraph() {
        return graph;
    }

    public Server getServer() {
        return server;
    }

    public static JSONArray verticesToJson(Iterable<Vertex> vertices) {
        JSONArray json = new JSONArray();
        for (Vertex v : vertices) {
            json.put(vertexToJson(v));
        }
        return json;
    }

    public static JSONObject vertexToJson(Vertex vertex) {
        JSONObject json = new JSONObject();
        json.put("id", vertex.getId().toString());

        JSONArray propertiesJson = new JSONArray();
        for (Property property : vertex.getProperties()) {
            propertiesJson.put(propertyYoJson(property));
        }
        json.put("properties", propertiesJson);

        return json;
    }

    public static JSONObject propertyYoJson(Property property) {
        JSONObject json = new JSONObject();
        json.put("key", property.getKey());
        json.put("name", property.getName());
        json.put("metadata", propertyMetadataToJson(property.getMetadata()));
        json.put("visibility", property.getVisibility().toString());
        json.put("value", property.getValue().toString());
        return json;
    }

    public static JSONObject propertyMetadataToJson(Map<String, Object> metadata) {
        JSONObject json = new JSONObject();
        for (Map.Entry<String, Object> entry : metadata.entrySet()) {
            json.put(entry.getKey(), entry.getValue());
        }
        return json;
    }
}
