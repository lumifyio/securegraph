package org.securegraph.elasticsearch;

import org.securegraph.*;
import org.securegraph.id.IdGenerator;
import org.securegraph.search.SearchIndex;
import org.securegraph.util.ConvertingIterable;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.Map;

import static org.securegraph.util.IterableUtils.singleOrDefault;

public class ElasticSearchGraph extends GraphBase {
    public static ElasticSearchGraph create(Map config) {
        return create(new ElasticSearchGraphConfiguration(config));
    }

    public static ElasticSearchGraph create(ElasticSearchGraphConfiguration config) {
        IdGenerator idGenerator = config.createIdGenerator();
        SearchIndex searchIndex = config.createSearchIndex();
        return new ElasticSearchGraph(config, idGenerator, searchIndex);
    }

    public ElasticSearchGraph(ElasticSearchGraphConfiguration configuration, IdGenerator idGenerator, SearchIndex searchIndex) {
        super(configuration, idGenerator, searchIndex);
        if (!(searchIndex instanceof ElasticSearchSearchIndexBase)) {
            throw new SecureGraphException("search index " + searchIndex.getClass().getName() + " must extend " + ElasticSearchSearchIndexBase.class.getName());
        }
        if (!getSearchIndex().isStoreSourceData()) {
            throw new SecureGraphException("search index " + searchIndex.getClass().getName() + " must be configured to store source data");
        }
    }

    @Override
    public ElasticSearchSearchIndexBase getSearchIndex() {
        return (ElasticSearchSearchIndexBase) super.getSearchIndex();
    }

    @Override
    public VertexBuilder prepareVertex(String vertexId, Visibility visibility) {
        if (vertexId == null) {
            vertexId = getIdGenerator().nextId();
        }

        return new VertexBuilder(vertexId, visibility) {
            @Override
            public Vertex save(Authorizations authorizations) {
                ElasticSearchVertex vertex = new ElasticSearchVertex(ElasticSearchGraph.this, getVertexId(), getVisibility(), getProperties(), authorizations);
                getSearchIndex().addElement(ElasticSearchGraph.this, vertex, authorizations);
                return vertex;
            }
        };
    }

    @Override
    public Iterable<Vertex> getVertices(EnumSet<FetchHint> fetchHints, Authorizations authorizations) {
        return query(authorizations).vertices();
    }

    @Override
    public Iterable<Vertex> getVertices(Iterable<String> ids, EnumSet<FetchHint> fetchHints, final Authorizations authorizations) {
        Map<String, VertexQueryResult> vertexQueryResults = getSearchIndex().getVertex(ids, fetchHints, authorizations);
        return new ConvertingIterable<VertexQueryResult, Vertex>(vertexQueryResults.values()) {
            @Override
            protected Vertex convert(VertexQueryResult vertexQueryResult) {
                return new ElasticSearchVertex(ElasticSearchGraph.this, vertexQueryResult, authorizations);
            }
        };
    }

    @Override
    public Vertex getVertex(String vertexId, EnumSet<FetchHint> fetchHints, Authorizations authorizations) {
        ArrayList<String> vertexIds = new ArrayList<String>();
        vertexIds.add(vertexId);
        return singleOrDefault(getVertices(vertexIds, fetchHints, authorizations), null);
    }

    @Override
    public void removeVertex(Vertex vertex, Authorizations authorizations) {
        throw new RuntimeException("not implemented");
    }

    @Override
    public EdgeBuilder prepareEdge(String edgeId, Vertex outVertex, Vertex inVertex, String label, Visibility visibility) {
        throw new RuntimeException("not implemented");
    }

    @Override
    public Iterable<Edge> getEdges(EnumSet<FetchHint> fetchHints, Authorizations authorizations) {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void removeEdge(Edge edge, Authorizations authorizations) {
        throw new RuntimeException("not implemented");
    }

    @Override
    public boolean isVisibilityValid(Visibility visibility, Authorizations authorizations) {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void clearData() {
        throw new RuntimeException("not implemented");
    }
}
