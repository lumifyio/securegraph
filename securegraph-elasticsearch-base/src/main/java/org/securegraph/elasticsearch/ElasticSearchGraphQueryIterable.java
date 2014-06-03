package org.securegraph.elasticsearch;

import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.securegraph.Element;
import org.securegraph.query.*;

import java.util.HashMap;
import java.util.Map;

public class ElasticSearchGraphQueryIterable<T extends Element> extends DefaultGraphQueryIterable<T> implements
        IterableWithTotalHits<T>,
        IterableWithSearchTime<T>,
        IterableWithScores<T> {
    private final long totalHits;
    private final long searchTimeInNanoSeconds;
    private final Map<Object, Double> scores = new HashMap<Object, Double>();

    public ElasticSearchGraphQueryIterable(QueryBase.Parameters parameters, Iterable<T> iterable, boolean evaluateQueryString, boolean evaluateHasContainers, long totalHits, long searchTimeInNanoSeconds, SearchHits hits) {
        super(parameters, iterable, evaluateQueryString, evaluateHasContainers);
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
}
