package org.securegraph.examples;

import com.altamiracorp.miniweb.App;
import com.altamiracorp.miniweb.HandlerChain;
import org.json.JSONArray;
import org.json.JSONObject;
import org.securegraph.Authorizations;
import org.securegraph.Graph;
import org.securegraph.Vertex;
import org.securegraph.Visibility;
import org.securegraph.query.*;

import javax.servlet.Servlet;
import javax.servlet.ServletConfig;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.Date;
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
                getGraph().prepareVertex(visibility)
                        .setProperty("age", random.nextInt(100), visibility)
                        .setProperty("publishedDate", new Date(1404159116647L + random.nextInt(1000000)), visibility)
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
        getGraph().defineProperty("age")
                .dataType(Integer.class)
                .define();
        getGraph().defineProperty("publishedDate")
                .dataType(Date.class)
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
            String authorizationsString = getRequiredParameter(request, "authorizations");
            Authorizations authorizations = createAuthorizations(authorizationsString.split(","));

            String q = getRequiredParameter(request, "q");
            String field = getRequiredParameter(request, "field");
            String interval = getRequiredParameter(request, "interval");

            Query query = _this.getGraph()
                    .query(q, authorizations)
                    .limit(0);
            String HISTOGRAM_NAME = "hist";
            if (query instanceof GraphQueryWithHistogram) {
                ((GraphQueryWithHistogram) query).addHistogram(HISTOGRAM_NAME, field, interval);
            } else {
                throw new RuntimeException("query " + query.getClass().getName() + " does not support histograms");
            }
            Iterable<Vertex> vertices = query.vertices();

            if (!(vertices instanceof IterableWithHistogramResults)) {
                throw new RuntimeException("query results " + query.getClass().getName() + " does not support histograms");
            }
            HistogramResult histogramResult = ((IterableWithHistogramResults) vertices).getHistogramResults(HISTOGRAM_NAME);

            JSONObject json = new JSONObject();
            json.put("histogramResult", histogramResultToJson(histogramResult));

            response.getOutputStream().write(json.toString(2).getBytes());
        }

        private JSONObject histogramResultToJson(HistogramResult histogramResult) {
            JSONObject json = new JSONObject();

            JSONArray bucketsJson = new JSONArray();
            for (HistogramBucket bucket : histogramResult.getBuckets()) {
                JSONObject bucketJson = new JSONObject();
                Object key = bucket.getKey();
                if (key instanceof Date) {
                    key = ((Date) key).getTime();
                }
                bucketJson.put("key", key);
                bucketJson.put("count", bucket.getCount());
                bucketsJson.put(bucketJson);
            }
            json.put("buckets", bucketsJson);

            return json;
        }
    }
}
