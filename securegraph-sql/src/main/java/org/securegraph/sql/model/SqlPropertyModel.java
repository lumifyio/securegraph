package org.securegraph.sql.model;

import org.securegraph.Authorizations;
import org.securegraph.Metadata;
import org.securegraph.Property;
import org.securegraph.Visibility;
import org.securegraph.property.MutablePropertyImpl;
import org.securegraph.sql.SqlGraph;
import org.securegraph.sql.serializer.ValueSerializer;
import org.securegraph.util.ConvertingIterable;
import org.securegraph.util.Group;
import org.securegraph.util.GroupingIterable;
import org.securegraph.util.LookAheadIterable;

import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static org.securegraph.util.IterableUtils.toSet;

public class SqlPropertyModel {
    private String key;
    private String name;
    private String visibility;
    private byte[] value;
    private String metadata_key;
    private byte[] metadata_value;
    private String metadata_visibility;
    private String hidden_visibility;

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getVisibility() {
        return visibility;
    }

    public void setVisibility(String visibility) {
        this.visibility = visibility;
    }

    public byte[] getValue() {
        return value;
    }

    public void setValue(byte[] value) {
        this.value = value;
    }

    public String getMetadata_key() {
        return metadata_key;
    }

    public void setMetadata_key(String metadata_key) {
        this.metadata_key = metadata_key;
    }

    public byte[] getMetadata_value() {
        return metadata_value;
    }

    public void setMetadata_value(byte[] metadata_value) {
        this.metadata_value = metadata_value;
    }

    public String getMetadata_visibility() {
        return metadata_visibility;
    }

    public void setMetadata_visibility(String metadata_visibility) {
        this.metadata_visibility = metadata_visibility;
    }

    public String getHidden_visibility() {
        return hidden_visibility;
    }

    public void setHidden_visibility(String hidden_visibility) {
        this.hidden_visibility = hidden_visibility;
    }

    public static Iterator<Property> toProperties(final SqlGraph sqlGraph, List<SqlPropertyModel> sqlPropertyModels, Authorizations authorizations) {
        Iterable<Group<SqlPropertyModel>> groupedSqlPropertyModels = new GroupingIterable<SqlPropertyModel>(sqlPropertyModels) {
            @Override
            protected String getKey(SqlPropertyModel obj) {
                return obj.getKey() + obj.getName() + obj.getVisibility();
            }
        };
        return new ConvertingIterable<Group<SqlPropertyModel>, Property>(groupedSqlPropertyModels) {
            @Override
            protected Property convert(Group<SqlPropertyModel> sqlPropertyModelGroup) {
                SqlPropertyModel m = sqlPropertyModelGroup.getItems().get(0);
                Object value = sqlGraph.getValueSerializer().toObject(m.getValue());
                Visibility visibility = new Visibility(m.getVisibility());
                Set<Visibility> hiddenVisibilities = getHiddenVisibilities(sqlPropertyModelGroup);
                Metadata metadata = getMetadata(sqlPropertyModelGroup, sqlGraph.getValueSerializer());
                return new MutablePropertyImpl(m.getKey(), m.getName(), value, metadata, hiddenVisibilities, visibility);
            }
        }.iterator();
    }

    private static Metadata getMetadata(Group<SqlPropertyModel> sqlPropertyModelGroup, ValueSerializer valueSerializer) {
        Metadata metadata = new Metadata();
        for (SqlPropertyModel m : sqlPropertyModelGroup.getItems()) {
            if (m.getMetadata_key() != null) {
                Object value = valueSerializer.toObject(m.getMetadata_value());
                metadata.add(m.getMetadata_key(), value, new Visibility(m.getMetadata_visibility()));
            }
        }
        return metadata;
    }

    private static Set<Visibility> getHiddenVisibilities(final Group<SqlPropertyModel> sqlPropertyModelGroup) {
        return toSet(new LookAheadIterable<SqlPropertyModel, Visibility>() {
            @Override
            protected boolean isIncluded(SqlPropertyModel src, Visibility visibility) {
                return visibility != null;
            }

            @Override
            protected Visibility convert(SqlPropertyModel o) {
                if (o.getHidden_visibility() == null) {
                    return null;
                }
                return new Visibility(o.getHidden_visibility());
            }

            @Override
            protected Iterator<SqlPropertyModel> createIterator() {
                return sqlPropertyModelGroup.getItems().iterator();
            }
        });
    }
}
