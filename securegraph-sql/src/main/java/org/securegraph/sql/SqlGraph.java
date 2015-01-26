package org.securegraph.sql;

import liquibase.database.DatabaseConnection;
import liquibase.exception.DatabaseException;
import org.securegraph.*;
import org.securegraph.event.AddPropertyEvent;
import org.securegraph.event.AddVertexEvent;
import org.securegraph.event.GraphEvent;
import org.securegraph.id.IdGenerator;
import org.securegraph.search.IndexHint;
import org.securegraph.search.SearchIndex;
import org.securegraph.sql.model.SqlMetadataModel;
import org.securegraph.sql.model.SqlPropertyModel;
import org.securegraph.sql.model.SqlVertexModel;
import org.securegraph.sql.serializer.ValueSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sql2o.Connection;
import org.sql2o.Sql2o;

import java.util.*;

public class SqlGraph extends GraphBaseWithSearchIndex {
    private static final Logger LOGGER = LoggerFactory.getLogger(SqlGraph.class);
    private final Sql2o sql2o;
    private final ValueSerializer valueSerializer;
    private final Queue<GraphEvent> graphEventQueue = new LinkedList<>();

    public SqlGraph(SqlGraphConfiguration configuration, IdGenerator idGenerator, SearchIndex searchIndex, ValueSerializer valueSerializer) {
        super(configuration, idGenerator, searchIndex);
        this.valueSerializer = valueSerializer;
        sql2o = new Sql2o(configuration.getDataSource());
    }


    public static SqlGraph create(SqlGraphConfiguration config) {
        if (config == null) {
            throw new IllegalArgumentException("config cannot be null");
        }
        SearchIndex searchIndex = config.createSearchIndex();
        IdGenerator idGenerator = config.createIdGenerator();
        ValueSerializer valueSerializer = config.createValueSerializer();
        SqlGraph graph = new SqlGraph(config, idGenerator, searchIndex, valueSerializer);
        graph.setup();
        return graph;
    }

    @Override
    protected void setup() {
        DatabaseConnection conn = getConfiguration().createMigrationConnection();
        try {
            new Migrate().migrate(conn);
        } catch (Throwable ex) {
            try {
                conn.close();
            } catch (DatabaseException exClose) {
                LOGGER.error("Could not close database", exClose);
            }
            throw ex;
        }
        super.setup();
    }

    @Override
    public SqlGraphConfiguration getConfiguration() {
        return (SqlGraphConfiguration) super.getConfiguration();
    }

    @Override
    public VertexBuilder prepareVertex(String vertexId, Visibility visibility) {
        if (vertexId == null) {
            vertexId = getIdGenerator().nextId();
        }

        return new VertexBuilder(vertexId, visibility) {
            @Override
            public Vertex save(Authorizations authorizations) {
                SqlVertex vertex = new SqlVertex(SqlGraph.this, getVertexId(), getVisibility(), getProperties(), null, authorizations);

                try (Connection conn = sql2o.beginTransaction()) {
                    saveVertex(conn, vertex);
                    conn.commit();
                    flush();
                }

                if (getIndexHint() != IndexHint.DO_NOT_INDEX) {
                    getSearchIndex().addElement(SqlGraph.this, vertex, authorizations);
                }

                if (hasEventListeners()) {
                    queueEvent(new AddVertexEvent(SqlGraph.this, vertex));
                    for (Property property : getProperties()) {
                        queueEvent(new AddPropertyEvent(SqlGraph.this, vertex, property));
                    }
                }

                return vertex;
            }
        };
    }

    @Override
    public void flush() {
        if (hasEventListeners()) {
            synchronized (this.graphEventQueue) {
                super.flush();
                flushGraphEventQueue();
            }
        } else {
            super.flush();
        }
    }

    private void flushGraphEventQueue() {
        GraphEvent graphEvent;
        while ((graphEvent = this.graphEventQueue.poll()) != null) {
            fireGraphEvent(graphEvent);
        }
    }

    private void saveVertex(Connection conn, SqlVertex vertex) {
        String vertexVisibilityString = vertex.getVisibility().getVisibilityString();
        Long vertexCount = (Long) conn
                .createQuery("SELECT count(1) FROM vertex WHERE id=:id AND visibility=:visibility")
                .addParameter("id", vertex.getId())
                .addParameter("visibility", vertexVisibilityString)
                .executeScalar();
        if (vertexCount == 0) {
            conn.createQuery("INSERT INTO vertex(id, visibility) VALUES (:id, :visibility)")
                    .addParameter("id", vertex.getId())
                    .addParameter("visibility", vertexVisibilityString)
                    .executeUpdate();
        } else {
            // TODO vertex already exists, no update needed?
        }

        // TODO save properties, etc.
    }

    @Override
    public Iterable<Vertex> getVertices(EnumSet<FetchHint> fetchHints, Authorizations authorizations) {
        try (Connection conn = sql2o.open()) {
            String sql = "SELECT v.id, v.visibility, vhv.visibility as vertex_hidden_visibility FROM " +
                    "vertex as v " +
                    "LEFT JOIN vertex_hidden_visibility as vhv ON (v.id=vhv.vertex_id AND v.visibility=vhv.vertex_visibility)";
            List<SqlVertexModel> vertices = conn.createQuery(sql).executeAndFetch(SqlVertexModel.class);
            return SqlVertexModel.toVertex(this, vertices, authorizations);
        }
    }

    @Override
    public void removeVertex(Vertex vertex, Authorizations authorizations) {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public EdgeBuilder prepareEdge(String edgeId, Vertex outVertex, Vertex inVertex, String label, Visibility visibility) {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public EdgeBuilderByVertexId prepareEdge(String edgeId, String outVertexId, String inVertexId, String label, Visibility visibility) {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public Iterable<Edge> getEdges(EnumSet<FetchHint> fetchHints, Authorizations authorizations) {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void removeEdge(Edge edge, Authorizations authorizations) {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public boolean isVisibilityValid(Visibility visibility, Authorizations authorizations) {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public Iterable<GraphMetadataEntry> getMetadata() {
        try (Connection conn = sql2o.open()) {
            return SqlMetadataModel.toGraphMetadataEntry(
                    getValueSerializer(),
                    conn.createQuery("SELECT * FROM metadata").executeAndFetch(SqlMetadataModel.class)
            );
        }
    }

    public ValueSerializer getValueSerializer() {
        return this.valueSerializer;
    }

    @Override
    public void clearData() {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void markVertexHidden(Vertex vertex, Visibility visibility, Authorizations authorizations) {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void markVertexVisible(Vertex vertex, Visibility visibility, Authorizations authorizations) {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void markEdgeHidden(Edge edge, Visibility visibility, Authorizations authorizations) {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void markEdgeVisible(Edge edge, Visibility visibility, Authorizations authorizations) {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void setMetadata(String key, Object value) {
        byte[] valueBytes = getValueSerializer().toBytes(value);

        try (Connection conn = sql2o.beginTransaction()) {
            Long keyExists = (Long) conn
                    .createQuery("SELECT count(1) FROM metadata WHERE key = :key")
                    .addParameter("key", key)
                    .executeScalar();
            if (keyExists == 0) {
                conn.createQuery("INSERT INTO metadata(key, value) VALUES (:key, :value)")
                        .addParameter("key", key)
                        .addParameter("value", valueBytes)
                        .executeUpdate();
            } else {
                conn.createQuery("UPDATE metadata SET value=:value WHERE key=:key")
                        .addParameter("key", key)
                        .addParameter("value", valueBytes)
                        .executeUpdate();
            }
            conn.commit();
        }
    }

    private void queueEvent(GraphEvent graphEvent) {
        synchronized (this.graphEventQueue) {
            this.graphEventQueue.add(graphEvent);
        }
    }

    public Iterator<Property> getVertexProperties(String vertexId, String vertexVisibility, Authorizations authorizations) {
        try (Connection conn = sql2o.open()) {
            String sql = "SELECT vp.key, vp.name, vp.visibility, vp.value, vpm.key as metadata_key, vpm.value as metadata_value, vpm.visibility as metadata_visibility, vphv.visibility as hidden_visibility FROM " +
                    "vertex_property as vp " +
                    "LEFT JOIN vertex_property_metadata as vpm ON (vp.vertex_id=vpm.vertex_id AND vp.vertex_visibility=vpm.vertex_visibility AND vp.key=vpm.property_key AND vp.name=vpm.property_name AND vp.visibility=vpm.property_visibility) "
                    + "LEFT JOIN vertex_property_hidden_visibility as vphv ON (vp.vertex_id=vphv.vertex_id AND vp.vertex_visibility=vphv.vertex_visibility AND vp.key=vphv.property_key AND vp.name=vphv.property_name AND vp.visibility=vphv.property_visibility) " +
                    "WHERE vp.vertex_id=:vertex_id AND vp.vertex_visibility=:vertex_visibility";
            List<SqlPropertyModel> properties = conn.createQuery(sql)
                    .addParameter("vertex_id", vertexId)
                    .addParameter("vertex_visibility", vertexVisibility)
                    .executeAndFetch(SqlPropertyModel.class);
            return SqlPropertyModel.toProperties(this, properties, authorizations);
        }
    }
}
