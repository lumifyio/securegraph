package com.altamiracorp.securegraph.accumulo.mapreduce;

import com.altamiracorp.securegraph.*;
import com.altamiracorp.securegraph.accumulo.*;
import com.altamiracorp.securegraph.accumulo.serializer.ValueSerializer;
import com.altamiracorp.securegraph.id.IdGenerator;
import org.apache.accumulo.core.data.Mutation;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.net.URISyntaxException;

public abstract class ElementMapper<KEYIN, VALUEIN> extends Mapper<KEYIN, VALUEIN, Text, Mutation> {
    public static final String GRAPH_CONFIG_PREFIX = "graphConfigPrefix";
    private ElementMutationBuilder elementMutationBuilder;

    @Override
    protected void setup(final Context context) throws IOException, InterruptedException {
        super.setup(context);
        String configPrefix = context.getConfiguration().get(GRAPH_CONFIG_PREFIX, "");
        AccumuloGraphConfiguration accumuloGraphConfiguration = new AccumuloGraphConfiguration(context.getConfiguration(), configPrefix);
        String tableNamePrefix = accumuloGraphConfiguration.getTableNamePrefix();
        final Text edgesTableName = new Text(AccumuloGraph.getEdgesTableName(tableNamePrefix));
        final Text dataTableName = new Text(AccumuloGraph.getDataTableName(tableNamePrefix));
        final Text verticesTableName = new Text(AccumuloGraph.getVerticesTableName(tableNamePrefix));
        ValueSerializer valueSerializer = accumuloGraphConfiguration.createValueSerializer();
        long maxStreamingPropertyValueTableDataSize = accumuloGraphConfiguration.getMaxStreamingPropertyValueTableDataSize();
        String dataDir = accumuloGraphConfiguration.getDataDir();
        FileSystem fileSystem;
        try {
            fileSystem = accumuloGraphConfiguration.createFileSystem();
        } catch (URISyntaxException e) {
            throw new IOException("Could not initialize", e);
        }

        this.elementMutationBuilder = new ElementMutationBuilder(fileSystem, valueSerializer, maxStreamingPropertyValueTableDataSize, dataDir) {
            @Override
            protected void saveVertexMutation(Mutation m) {
                try {
                    context.write(verticesTableName, m);
                } catch (Exception e) {
                    throw new RuntimeException("Could not save vertex", e);
                }
            }

            @Override
            protected void saveEdgeMutation(Mutation m) {
                try {
                    context.write(edgesTableName, m);
                } catch (Exception e) {
                    throw new RuntimeException("Could not save edge", e);
                }
            }

            @Override
            protected void saveDataMutation(Mutation m) {
                try {
                    context.write(dataTableName, m);
                } catch (Exception e) {
                    throw new RuntimeException("Could not save data", e);
                }
            }
        };
    }

    public VertexBuilder prepareVertex(Object vertexId, Visibility visibility, Authorizations authorizations) {
        if (vertexId == null) {
            vertexId = getIdGenerator().nextId();
        }

        return new VertexBuilder(vertexId, visibility) {
            @Override
            public Vertex save() {
                AccumuloVertex vertex = new AccumuloVertex(null, getVertexId(), getVisibility(), getProperties());
                elementMutationBuilder.saveVertex(vertex);
                return vertex;
            }
        };
    }

    public Edge addEdge(Object edgeId, Vertex outVertex, Vertex inVertex, String label, Visibility visibility, Authorizations authorizations) {
        return prepareEdge(edgeId, outVertex, inVertex, label, visibility, authorizations).save();
    }

    public EdgeBuilder prepareEdge(Object edgeId, Vertex outVertex, Vertex inVertex, String label, Visibility visibility, Authorizations authorizations) {
        if (edgeId == null) {
            edgeId = getIdGenerator().nextId();
        }

        return new EdgeBuilder(edgeId, outVertex, inVertex, label, visibility) {
            @Override
            public Edge save() {
                AccumuloEdge edge = new AccumuloEdge(null, getEdgeId(), getOutVertex().getId(), getInVertex().getId(), getLabel(), getVisibility(), getProperties());
                elementMutationBuilder.saveEdge(edge);
                return edge;
            }
        };
    }

    protected abstract IdGenerator getIdGenerator();
}
