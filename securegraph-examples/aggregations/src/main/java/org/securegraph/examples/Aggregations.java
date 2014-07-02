package org.securegraph.examples;

import com.altamiracorp.miniweb.App;
import com.altamiracorp.miniweb.HandlerChain;
import org.json.JSONArray;
import org.json.JSONObject;
import org.securegraph.Authorizations;
import org.securegraph.Graph;
import org.securegraph.Vertex;
import org.securegraph.examples.dataset.BabyNamesDataset;
import org.securegraph.query.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.Servlet;
import javax.servlet.ServletConfig;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Date;

import static org.securegraph.util.IterableUtils.count;

public class Aggregations extends ExampleBase {
    private static final Logger LOGGER = LoggerFactory.getLogger(Aggregations.class);
    private static Aggregations _this;
    private static final String VISIBILITIES[] = new String[]{"a", "b", "c", "d"};
    private static final int VERTICES_TO_CREATE = 3000;

    public static void main(String[] args) throws Exception {
        LOGGER.debug("begin " + Aggregations.class.getName());
        _this = new Aggregations();
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
    protected void populateData() throws IOException {
        if (count(getGraph().getVertices(createAuthorizations(VISIBILITIES))) >= VERTICES_TO_CREATE) {
            LOGGER.debug("skipping create data. data already exists");
            return;
        }

        addAuthorizations();
        populateVertices();
    }

    private void populateVertices() throws IOException {
        BabyNamesDataset.load(getGraph(), VERTICES_TO_CREATE, VISIBILITIES, createAuthorizations());
        //ImdbDataset.load(getGraph(), VERTICES_TO_CREATE, VISIBILITIES, createAuthorizations());
    }

    private void addAuthorizations() {
        for (String v : VISIBILITIES) {
            addAuthorizationToUser(v);
        }
    }

    public static class Router extends RouterBase {
        @Override
        protected void initApp(ServletConfig config, App app) {
            super.initApp(config, app);

            app.get("/search", Search.class);
        }
    }

    public static enum QueryType {
        HISTOGRAM,
        TERMS
    }

    public static class Search extends HandlerBase {

        @Override
        public void handle(HttpServletRequest request, HttpServletResponse response, HandlerChain handlerChain) throws Exception {
            String authorizationsString = getRequiredParameter(request, "authorizations");
            Authorizations authorizations = createAuthorizations(authorizationsString.split(","));

            String q = getRequiredParameter(request, "q");
            String field = getRequiredParameter(request, "field");
            String interval = getRequiredParameter(request, "interval");
            QueryType queryType = getQueryType(request, "querytype");

            String AGGREGATION_NAME = "agg";

            Iterable<Vertex> vertices = queryForVertices(AGGREGATION_NAME, q, field, interval, queryType, authorizations);

            JSONObject json = new JSONObject();
            if (vertices instanceof IterableWithTotalHits) {
                json.put("totalHits", ((IterableWithTotalHits) vertices).getTotalHits());
            }
            if (queryType == QueryType.HISTOGRAM) {
                json.put("histogramResult", histogramResultToJson(getHistogramResult(vertices, AGGREGATION_NAME)));
            } else if (queryType == QueryType.TERMS) {
                json.put("termsResult", termsResultToJson(getTermsResult(vertices, AGGREGATION_NAME)));
            }

            response.getOutputStream().write(json.toString(2).getBytes());
        }

        private QueryType getQueryType(HttpServletRequest request, String parameterName) {
            String queryType = getRequiredParameter(request, parameterName);
            return QueryType.valueOf(queryType);
        }

        private static Iterable<Vertex> queryForVertices(String histogramName, String q, String field, String interval, QueryType queryType, Authorizations authorizations) {
            Query query = _this.getGraph()
                    .query(q, authorizations)
                    .limit(0);
            if (queryType == QueryType.TERMS && query instanceof GraphQueryWithTermsAggregation) {
                ((GraphQueryWithTermsAggregation) query).addTermsAggregation(histogramName, field);
            } else if (queryType == QueryType.HISTOGRAM && query instanceof GraphQueryWithHistogramAggregation) {
                ((GraphQueryWithHistogramAggregation) query).addHistogramAggregation(histogramName, field, interval);
            } else {
                throw new RuntimeException("query " + query.getClass().getName() + " does not support " + queryType);
            }
            return query.vertices();
        }

        private static HistogramResult getHistogramResult(Iterable<Vertex> vertices, String aggregationName) {
            if (!(vertices instanceof IterableWithHistogramResults)) {
                throw new RuntimeException("query results " + vertices.getClass().getName() + " does not support histograms");
            }
            return ((IterableWithHistogramResults) vertices).getHistogramResults(aggregationName);
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

        private static TermsResult getTermsResult(Iterable<Vertex> vertices, String aggregationName) {
            if (!(vertices instanceof IterableWithTermsResults)) {
                throw new RuntimeException("query results " + vertices.getClass().getName() + " does not support terms");
            }
            return ((IterableWithTermsResults) vertices).getTermsResults(aggregationName);
        }

        private JSONObject termsResultToJson(TermsResult termsResult) {
            JSONObject json = new JSONObject();

            JSONArray bucketsJson = new JSONArray();
            for (TermsBucket bucket : termsResult.getBuckets()) {
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
