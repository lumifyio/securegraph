package org.securegraph.elasticsearch;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogram;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.securegraph.Element;
import org.securegraph.SecureGraphException;
import org.securegraph.query.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ElasticSearchGraphQueryIterable<T extends Element> extends DefaultGraphQueryIterable<T> implements
        IterableWithTotalHits<T>,
        IterableWithSearchTime<T>,
        IterableWithScores<T>,
        IterableWithHistogramResults<T> {
    private final SearchResponse searchResponse;
    private final long totalHits;
    private final long searchTimeInNanoSeconds;
    private final Map<Object, Double> scores = new HashMap<Object, Double>();

    public ElasticSearchGraphQueryIterable(SearchResponse searchResponse, QueryBase.Parameters parameters, Iterable<T> iterable, boolean evaluateQueryString, boolean evaluateHasContainers, long totalHits, long searchTimeInNanoSeconds, SearchHits hits) {
        super(parameters, iterable, evaluateQueryString, evaluateHasContainers);
        this.searchResponse = searchResponse;
        this.totalHits = totalHits;
        this.searchTimeInNanoSeconds = searchTimeInNanoSeconds;
        for (SearchHit hit : hits.getHits()) {
            scores.put(hit.getId(), (double) hit.getScore());
        }
    }

    @Override
    public long getTotalHits() {
        return this.totalHits;
    }

    @Override
    public Map<Object, Double> getScores() {
        return this.scores;
    }

    @Override
    public long getSearchTimeNanoSeconds() {
        return this.searchTimeInNanoSeconds;
    }

    @Override
    public HistogramResult getHistogramResults(String name) {
        List<HistogramBucket> buckets = new ArrayList<HistogramBucket>();
        Aggregation agg = this.searchResponse.getAggregations().get(name);
        if (agg == null) {
            return null;
        }
        if (agg instanceof DateHistogram) {
            DateHistogram h = (DateHistogram) agg;
            for (DateHistogram.Bucket b : h.getBuckets()) {
                buckets.add(new HistogramBucket(b.getKeyAsDate().toDate(), b.getDocCount()));
            }
        } else if (agg instanceof Histogram) {
            Histogram h = (Histogram) agg;
            for (Histogram.Bucket b : h.getBuckets()) {
                buckets.add(new HistogramBucket(b.getKey(), b.getDocCount()));
            }
        } else {
            throw new SecureGraphException("Aggregation is not a histogram: " + agg.getClass().getName());
        }
        return new HistogramResult(buckets);
    }
}
