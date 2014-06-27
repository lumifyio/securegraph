package org.securegraph.examples;

import com.altamiracorp.miniweb.App;
import com.altamiracorp.miniweb.HandlerChain;
import org.json.JSONObject;
import org.securegraph.Authorizations;
import org.securegraph.Graph;
import org.securegraph.Vertex;
import org.securegraph.Visibility;
import org.securegraph.type.GeoPoint;

import javax.servlet.Servlet;
import javax.servlet.ServletConfig;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.Random;

import static org.securegraph.util.IterableUtils.count;

public class Histogram extends ExampleBase {
    private static Histogram _this;
    private static final String VISIBILITIES[] = new String[]{"a", "b", "c", "d"};
    private static final int VERTICES_TO_CREATE = 10000;

    public static void main(String[] args) throws Exception {
        _this = new Histogram();
        _this.run(args);
    }

    @Override
    protected Class<? extends Servlet> getServletClass() {
        return Router.class;
    }

    @Override
    protected void clearGraph(Graph graph) {
        int count = count(graph.getVertices(createAuthorizations(VISIBILITIES)));
        if (count >= VERTICES_TO_CREATE) {
            LOGGER.debug("skipping clear graph. data already exists. count: " + count);
            return;
        }
        LOGGER.debug("clearing " + count + " vertices");
        super.clearGraph(graph);
    }

    @Override
    protected void populateData() {
        if (count(getGraph().getVertices(createAuthorizations(VISIBILITIES))) >= VERTICES_TO_CREATE) {
            LOGGER.debug("skipping create data. data already exists");
            return;
        }

        defineTypes();
        addAuthorizations();
        populateVertices();
    }

    private void populateVertices() {
        LOGGER.debug("populating data count: " + VERTICES_TO_CREATE);
        Authorizations authorizations = createAuthorizations();
        Random random = new Random(1000);
        for (int i = 0; i < VERTICES_TO_CREATE; ) {
            for (String v : VISIBILITIES) {
                if (i % 1000 == 0) {
                    LOGGER.debug("populating data " + i + "/" + VERTICES_TO_CREATE);
                }
                Visibility visibility = new Visibility(v);
                double lat = random.nextDouble();
                double lon = random.nextDouble();
                getGraph().prepareVertex(visibility)
                        .setProperty("geolocation", new GeoPoint(lat, lon), visibility)
                        .save(authorizations);
                i++;
            }
        }
        getGraph().flush();
        LOGGER.debug("populated data");
    }

    private void addAuthorizations() {
        for (String v : VISIBILITIES) {
            addAuthorizationToUser(v);
        }
    }

    private void defineTypes() {
        getGraph().defineProperty("geolocation")
                .dataType(GeoPoint.class)
                .define();
        getGraph().flush();
    }

    public static class Router extends RouterBase {
        @Override
        protected void initApp(ServletConfig config, App app) {
            super.initApp(config, app);

            app.get("/search", Search.class);
        }
    }

    public static class Search extends HandlerBase {

        @Override
        public void handle(HttpServletRequest request, HttpServletResponse response, HandlerChain handlerChain) throws Exception {
            String authorizations = getRequiredParameter(request, "authorizations");
            String query = getRequiredParameter(request, "q");

            Iterable<Vertex> vertices = _this.getGraph()
                    .query(query, createAuthorizations(authorizations.split(",")))
                    .vertices();

            JSONObject json = new JSONObject();
            json.put("vertices", verticesToJson(vertices));

            response.getOutputStream().write(json.toString(2).getBytes());
        }
    }
}
