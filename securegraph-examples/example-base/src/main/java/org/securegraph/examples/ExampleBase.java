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
import org.securegraph.util.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.Servlet;
import java.io.*;
import java.util.*;
import java.util.zip.GZIPInputStream;

public abstract class ExampleBase {
    protected static final Logger LOGGER = LoggerFactory.getLogger(ExampleBase.class);
    public static final String CATEGORY_ID_PREFIX = "CATEGORY_";
    public static final Object MOVIE_ID_PREFIX = "MOVIE_";

    @Parameter(names = "-port", description = "Port to run server on")
    private int port = 7777;

    private Server server;
    private Graph graph;

    protected void run(String[] args) throws Exception {
        new JCommander(this, args);

        this.server = runJetty(port);
        this.graph = openGraph(getGraphConfig());
        clearGraph(this.graph);
        populateData();
    }

    protected void populateData() throws IOException {

    }

    protected void clearGraph(Graph graph) {
        graph.clearData();
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

    protected void loadBabyNamesDataSet(int numberOfVerticesToCreate, String[] visibilities) throws IOException {
        LOGGER.debug("populating data count: " + numberOfVerticesToCreate);
        Authorizations authorizations = createAuthorizations();

        File file = new File("../baby-names.txt.gz");
        BufferedReader br = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(file))));
        try {
            int i = 0;
            String line;
            while (i < numberOfVerticesToCreate && (line = br.readLine()) != null) {
                if (i % 1000 == 0) {
                    LOGGER.debug("populating data " + i + "/" + numberOfVerticesToCreate);
                }
                String[] lineParts = line.split(",");
                if (lineParts.length != 4) {
                    continue;
                }
                int year = Integer.parseInt(lineParts[0]);
                String name = lineParts[1];
                int sex = lineParts[2].trim().equals("M") ? 1 : 0;
                int count = Integer.parseInt(lineParts[3]);
                Visibility visibility = new Visibility(visibilities[i % visibilities.length]);
                GregorianCalendar c = new GregorianCalendar();
                c.set(year, Calendar.JANUARY, 1, 1, 1, 1);
                c.set(Calendar.MILLISECOND, 0);
                getGraph().prepareVertex(visibility)
                        .setProperty("year", c.getTime(), visibility)
                        .setProperty("name", name, visibility)
                        .setProperty("sex", sex, visibility)
                        .setProperty("count", count, visibility)
                        .save(authorizations);
                i++;
            }
        } finally {
            br.close();
        }
        getGraph().flush();
        LOGGER.debug("populated data");
    }

    protected void loadImdbDataSet(int numberOfVerticesToCreate, String[] visibilities) throws FileNotFoundException {
        Authorizations authorizations = createAuthorizations();
        InputStream in = new FileInputStream(new File("../imdb.csv"));
        Map<String, Vertex> categoryCache = new HashMap<String, Vertex>();
        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(in));
            for (int i = 0; i < numberOfVerticesToCreate; i++) {
                String line = reader.readLine();
                if (line == null) {
                    throw new RuntimeException("Not enough lines in file. Needed " + numberOfVerticesToCreate + " found " + i);
                }
                String[] parts = line.split("\t");
                String title = parts[0];
                int year = Integer.parseInt(parts[1]);
                double rating = Double.parseDouble(parts[2]);
                String[] categoriesArray;
                if (parts.length < 5) {
                    categoriesArray = new String[0];
                } else {
                    categoriesArray = parts[4].split(",");
                }

                GregorianCalendar c = new GregorianCalendar();
                c.set(year, Calendar.JANUARY, 1, 1, 1, 1);
                c.set(Calendar.MILLISECOND, 0);
                Visibility visibility = new Visibility(visibilities[i % visibilities.length]);
                Vertex movieVertex = graph.prepareVertex(MOVIE_ID_PREFIX + title, visibility)
                        .setProperty("title", title, visibility)
                        .setProperty("year", c.getTime(), visibility)
                        .setProperty("rating", rating, visibility)
                        .save(authorizations);
                for (String category : categoriesArray) {
                    visibility = new Visibility("");
                    Vertex categoryVertex = categoryCache.get(category);
                    if (categoryVertex == null) {
                        categoryVertex = graph.prepareVertex(CATEGORY_ID_PREFIX + category, visibility)
                                .setProperty("title", category, visibility)
                                .save(authorizations);
                        categoryCache.put(category, categoryVertex);
                    }
                    graph.addEdge(categoryVertex.getId() + "->" + movieVertex.getId(), categoryVertex, movieVertex, "hasMovie", visibility, authorizations);
                }
            }
            graph.flush();
        } catch (Exception e) {
            throw new RuntimeException("Could not create vertices", e);
        } finally {
            try {
                in.close();
            } catch (IOException e) {
                // do nothing
            }
        }
    }
}
