package org.securegraph.sql.model;

import org.securegraph.Authorizations;
import org.securegraph.Vertex;
import org.securegraph.Visibility;
import org.securegraph.sql.LazyProperties;
import org.securegraph.sql.SqlGraph;
import org.securegraph.sql.SqlVertex;
import org.securegraph.util.ConvertingIterable;
import org.securegraph.util.Group;
import org.securegraph.util.GroupingIterable;
import org.securegraph.util.LookAheadIterable;

import java.util.Iterator;

public class SqlVertexModel {
    private String id;
    private String visibility;
    private String vertex_hidden_visibility;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getVisibility() {
        return visibility;
    }

    public void setVisibility(String visibility) {
        this.visibility = visibility;
    }

    public String getVertex_hidden_visibility() {
        return vertex_hidden_visibility;
    }

    public void setVertex_hidden_visibility(String vertex_hidden_visibility) {
        this.vertex_hidden_visibility = vertex_hidden_visibility;
    }

    public static Iterable<Vertex> toVertex(final SqlGraph sqlGraph, Iterable<SqlVertexModel> sqlVertexModels, final Authorizations authorizations) {
        Iterable<Group<SqlVertexModel>> groupedSqlVertexModels = new GroupingIterable<SqlVertexModel>(sqlVertexModels) {
            @Override
            protected String getKey(SqlVertexModel obj) {
                return obj.getId() + obj.getVisibility();
            }
        };
        return new ConvertingIterable<Group<SqlVertexModel>, Vertex>(groupedSqlVertexModels) {
            @Override
            protected Vertex convert(Group<SqlVertexModel> sqlVertexModelGroup) {
                SqlVertexModel m = sqlVertexModelGroup.getItems().get(0);
                Visibility visibility = new Visibility(m.getVisibility());
                Iterable<Visibility> hiddenVisibilities = getHiddenVisibilities(sqlVertexModelGroup);
                LazyProperties properties = new LazyProperties(sqlGraph, m.getId(), m.getVisibility(), authorizations);
                return new SqlVertex(sqlGraph, m.getId(), visibility, properties, hiddenVisibilities, authorizations);
            }
        };
    }

    private static Iterable<Visibility> getHiddenVisibilities(final Group<SqlVertexModel> sqlVertexModelGroup) {
        return new LookAheadIterable<SqlVertexModel, Visibility>() {
            @Override
            protected boolean isIncluded(SqlVertexModel src, Visibility visibility) {
                return visibility != null;
            }

            @Override
            protected Visibility convert(SqlVertexModel o) {
                if (o.getVertex_hidden_visibility() == null) {
                    return null;
                }
                return new Visibility(o.getVertex_hidden_visibility());
            }

            @Override
            protected Iterator<SqlVertexModel> createIterator() {
                return sqlVertexModelGroup.getItems().iterator();
            }
        };
    }
}
