package org.securegraph;

import org.securegraph.id.IdGenerator;
import org.securegraph.path.PathFindingAlgorithm;
import org.securegraph.path.RecursivePathFindingAlgorithm;
import org.securegraph.query.GraphQuery;
import org.securegraph.search.SearchIndex;
import org.securegraph.util.ToElementIterable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public abstract class GraphBaseWithSearchIndex extends GraphBase implements Graph {
    private static final Logger LOGGER = LoggerFactory.getLogger(GraphBaseWithSearchIndex.class);
    public static final String METADATA_DEFINE_PROPERTY_PREFIX = "defineProperty.";
    private final GraphConfiguration configuration;
    private final IdGenerator idGenerator;
    private SearchIndex searchIndex;
    private final PathFindingAlgorithm pathFindingAlgorithm = new RecursivePathFindingAlgorithm();

    protected GraphBaseWithSearchIndex(GraphConfiguration configuration, IdGenerator idGenerator, SearchIndex searchIndex) {
        this.configuration = configuration;
        this.idGenerator = idGenerator;
        this.searchIndex = searchIndex;
    }

    protected void setup() {
        setupGraphMetadata();
    }

    private void setupGraphMetadata() {
        for (GraphMetadataEntry graphMetadataEntry : getMetadata()) {
            setupGraphMetadata(graphMetadataEntry);
        }
    }

    protected void setupGraphMetadata(GraphMetadataEntry graphMetadataEntry) {
        Object v = graphMetadataEntry.getValue();
        if (graphMetadataEntry.getKey().startsWith(METADATA_DEFINE_PROPERTY_PREFIX)) {
            if (v instanceof PropertyDefinition) {
                setupPropertyDefinition((PropertyDefinition) v);
            } else {
                throw new SecureGraphException("Invalid property metadata: " + graphMetadataEntry.getKey());
            }
        }
    }

    protected void setupPropertyDefinition(PropertyDefinition propertyDefinition) {
        try {
            getSearchIndex().addPropertyDefinition(propertyDefinition);
        } catch (IOException e) {
            throw new SecureGraphException("Could not add property definition to search index", e);
        }
    }

    @Override
    public GraphQuery query(Authorizations authorizations) {
        return getSearchIndex().queryGraph(this, null, authorizations);
    }

    @Override
    public GraphQuery query(String queryString, Authorizations authorizations) {
        return getSearchIndex().queryGraph(this, queryString, authorizations);
    }

    public IdGenerator getIdGenerator() {
        return idGenerator;
    }

    public GraphConfiguration getConfiguration() {
        return configuration;
    }

    public SearchIndex getSearchIndex() {
        return searchIndex;
    }

    @Override
    public void reindex(Authorizations authorizations) {
        reindexVertices(authorizations);
        reindexEdges(authorizations);
    }

    protected void reindexVertices(Authorizations authorizations) {
        this.searchIndex.addElements(this, new ToElementIterable<Vertex>(getVertices(authorizations)), authorizations);
    }

    private void reindexEdges(Authorizations authorizations) {
        this.searchIndex.addElements(this, new ToElementIterable<Edge>(getEdges(authorizations)), authorizations);
    }

    @Override
    public void flush() {
        if (getSearchIndex() != null) {
            this.searchIndex.flush();
        }
    }

    @Override
    public void shutdown() {
        flush();
        if (getSearchIndex() != null) {
            this.searchIndex.shutdown();
            this.searchIndex = null;
        }
    }

    @Override
    public DefinePropertyBuilder defineProperty(final String propertyName) {
        return new DefinePropertyBuilder(propertyName) {
            @Override
            public PropertyDefinition define() {
                PropertyDefinition propertyDefinition = super.define();
                try {
                    getSearchIndex().addPropertyDefinition(propertyDefinition);
                } catch (IOException e) {
                    throw new SecureGraphException("Could not add property definition to search index", e);
                }
                setMetadata(METADATA_DEFINE_PROPERTY_PREFIX + propertyName, propertyDefinition);
                return propertyDefinition;
            }
        };
    }

    @Override
    public boolean isFieldBoostSupported() {
        return getSearchIndex().isFieldBoostSupported();
    }

    @Override
    public boolean isEdgeBoostSupported() {
        return getSearchIndex().isEdgeBoostSupported();
    }

    @Override
    public SearchIndexSecurityGranularity getSearchIndexSecurityGranularity() {
        return getSearchIndex().getSearchIndexSecurityGranularity();
    }
}
