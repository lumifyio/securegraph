package org.securegraph.elasticsearch;

import org.securegraph.*;
import org.securegraph.id.IdGenerator;
import org.securegraph.search.SearchIndex;

import java.util.EnumSet;
import java.util.Map;

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
    public Vertex getVertex(String vertexId, EnumSet<FetchHint> fetchHints, Authorizations authorizations) {
        VertexQueryResult vertexQueryResult = getSearchIndex().getVertex(vertexId, fetchHints, authorizations);
        return new ElasticSearchVertex(this, vertexQueryResult, authorizations);
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
