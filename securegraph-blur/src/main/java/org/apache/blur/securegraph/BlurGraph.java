/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.blur.securegraph;

import static org.apache.blur.securegraph.Constants.BLUR_CONNECTIONSTR;
import static org.apache.blur.securegraph.Constants.BLUR_TABLE;
import static org.apache.blur.securegraph.Constants.EDGE;
import static org.apache.blur.securegraph.Constants.EDGE_FAMILY;
import static org.apache.blur.securegraph.Constants.IN;
import static org.apache.blur.securegraph.Constants.LABEL;
import static org.apache.blur.securegraph.Constants.META;
import static org.apache.blur.securegraph.Constants.NAME;
import static org.apache.blur.securegraph.Constants.OUT;
import static org.apache.blur.securegraph.Constants.PROPERTY_FAMILY;
import static org.apache.blur.securegraph.Constants.TRUE;
import static org.apache.blur.securegraph.Constants.VERTEX;
import static org.apache.blur.securegraph.Constants.VERTEX_FAMILY;
import static org.apache.blur.securegraph.Constants.VISIBILITY;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thrift.BlurClient;
import org.apache.blur.thrift.generated.Blur.Iface;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.generated.Column;
import org.apache.blur.thrift.generated.Query;
import org.apache.blur.thrift.generated.Record;
import org.apache.blur.thrift.generated.RecordMutation;
import org.apache.blur.thrift.generated.RecordMutationType;
import org.apache.blur.thrift.generated.Row;
import org.apache.blur.thrift.generated.RowMutation;
import org.apache.blur.thrift.generated.RowMutationType;

import com.altamiracorp.securegraph.Authorizations;
import com.altamiracorp.securegraph.Edge;
import com.altamiracorp.securegraph.GraphBase;
import com.altamiracorp.securegraph.GraphConfiguration;
import com.altamiracorp.securegraph.Property;
import com.altamiracorp.securegraph.SecureGraphException;
import com.altamiracorp.securegraph.Vertex;
import com.altamiracorp.securegraph.Visibility;
import com.altamiracorp.securegraph.id.IdGenerator;
import com.altamiracorp.securegraph.property.PropertyBase;
import com.altamiracorp.securegraph.search.SearchIndex;

public class BlurGraph extends GraphBase {

  private final Iface _client;
  private final String _table;
  private final TypeLookup _lookup;

  protected BlurGraph(GraphConfiguration configuration, IdGenerator idGenerator, SearchIndex searchIndex) {
    super(configuration, idGenerator, searchIndex);
    String connection = configuration.getConfigString(BLUR_CONNECTIONSTR, null);
    _client = BlurClient.getClient(connection);
    _table = configuration.getConfigString(BLUR_TABLE, null);
    _lookup = new TypeLookup(_client, _table);
  }

  public TypeLookup getLookup() {
    return _lookup;
  }

  public Iface getClient() {
    return _client;
  }

  public String getTable() {
    return _table;
  }

  @Override
  public void flush() {
    // flush not required for Blur
  }

  @Override
  public void shutdown() {
    // shutdown not required for Blur
  }

  @Override
  public void removeVertex(Vertex vertex, Authorizations authorizations) {
    try {
      _client.mutate(deleteRow(vertex.getId()));
    } catch (BlurException e) {
      throw new RuntimeException(e);
    } catch (TException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void removeEdge(Edge edge, Authorizations authorizations) {
    try {
      _client.mutate(deleteRow(edge.getId()));
    } catch (BlurException e) {
      throw new RuntimeException(e);
    } catch (TException e) {
      throw new RuntimeException(e);
    }
  }

  private RowMutation deleteRow(Object rowId) {
    RowMutation rowMutation = new RowMutation();
    rowMutation.setTable(_table);
    rowMutation.setRowMutationType(RowMutationType.DELETE_ROW);
    rowMutation.setRowId(rowId.toString());
    return rowMutation;
  }

  @Override
  public Vertex addVertex(Object vertexId, Visibility visibility, Property... properties) {
    String vertexIdStr = vertexId.toString();

    // Create the vertex
    RowMutation rowMutation = new RowMutation();
    rowMutation.setRowId(vertexIdStr);
    rowMutation.setTable(_table);

    Record recordIn = new Record();
    recordIn.setRecordId(vertexIdStr);
    recordIn.setFamily(VERTEX_FAMILY);
    recordIn.addToColumns(new Column(VERTEX, TRUE));
    if (visibility != null) {
      recordIn.addToColumns(new Column(VISIBILITY, visibility.getVisibilityString()));
    }
    RecordMutation recordMutationInVertex = new RecordMutation();
    recordMutationInVertex.setRecord(recordIn);
    recordMutationInVertex.setRecordMutationType(RecordMutationType.REPLACE_ENTIRE_RECORD);

    // @TODO store props

    rowMutation.addToRecordMutations(recordMutationInVertex);
    addProperties(rowMutation, properties);

    try {
      _client.mutate(rowMutation);
    } catch (BlurException e) {
      throw new RuntimeException(e);
    } catch (TException e) {
      throw new RuntimeException(e);
    }

    return new BlurVertex(this, vertexIdStr, null);
  }

  private void addProperties(RowMutation rowMutation, Property... properties) {
    if (properties != null) {
      for (Property property : properties) {
        Record record = new Record();
        record.setRecordId(property.getId().toString());
        record.setFamily(PROPERTY_FAMILY);

        String name = property.getName();
        record.addToColumns(new Column(NAME, name));

        Object value = property.getValue();

        _lookup.addTypeIfNeeded(PROPERTY_FAMILY, name, value);
        record.addToColumns(new Column(name, value.toString()));

        Visibility visibility = property.getVisibility();
        if (visibility != null) {
          record.addToColumns(new Column(VISIBILITY, visibility.getVisibilityString()));
        }

        Map<String, Object> metadata = property.getMetadata();
        if (metadata != null) {
          for (Entry<String, Object> e : metadata.entrySet()) {
            String columnName = META + e.getKey();
            Object columnValue = e.getValue();
            _lookup.addTypeIfNeeded(PROPERTY_FAMILY, columnName, columnValue);
            record.addToColumns(new Column(columnName, columnValue.toString()));
          }
        }

        RecordMutation recordMutationProp = new RecordMutation();
        recordMutationProp.setRecord(record);
        recordMutationProp.setRecordMutationType(RecordMutationType.REPLACE_ENTIRE_RECORD);
        rowMutation.addToRecordMutations(recordMutationProp);
      }
    }
  }

  @Override
  public Edge addEdge(Object edgeId, Vertex outVertex, Vertex inVertex, String label, Visibility visibility,
      Property... properties) {
    String edgeRowId = edgeId.toString();
    String outVertexId = outVertex.getId().toString();
    String inVertexId = inVertex.getId().toString();

    // Create the edge
    RowMutation rowMutation = new RowMutation();
    rowMutation.setRowId(edgeRowId);
    rowMutation.setTable(_table);

    Record recordIn = new Record();
    recordIn.setRecordId(edgeRowId);
    recordIn.setFamily(EDGE_FAMILY);
    recordIn.addToColumns(new Column(EDGE, TRUE));
    recordIn.addToColumns(new Column(IN, inVertexId));
    recordIn.addToColumns(new Column(OUT, outVertexId));
    if (label != null) {
      recordIn.addToColumns(new Column(LABEL, label));
    }
    if (visibility != null) {
      recordIn.addToColumns(new Column(VISIBILITY, visibility.getVisibilityString()));
    }
    RecordMutation recordMutationInVertex = new RecordMutation();
    recordMutationInVertex.setRecord(recordIn);
    recordMutationInVertex.setRecordMutationType(RecordMutationType.REPLACE_ENTIRE_RECORD);

    // @TODO store props
    rowMutation.addToRecordMutations(recordMutationInVertex);
    addProperties(rowMutation, properties);
    try {
      _client.mutate(rowMutation);
    } catch (BlurException e) {
      throw new RuntimeException(e);
    } catch (TException e) {
      throw new RuntimeException(e);
    }
    return new BlurEdge(this, edgeRowId, null);
  }

  @Override
  public Iterable<Vertex> getVertices(final Authorizations authorizations) throws SecureGraphException {
    Query query = new Query();
    query.setRowQuery(true);
    query.setQuery("+<" + VERTEX_FAMILY + "." + VERTEX + ":" + TRUE + ">");
    final RowIterable rowIterable = new RowIterable(_client, _table, query, null);
    return new Iterable<Vertex>() {

      @Override
      public Iterator<Vertex> iterator() {
        final Iterator<Row> iterator = rowIterable.iterator();
        return new Iterator<Vertex>() {

          @Override
          public boolean hasNext() {
            return iterator.hasNext();
          }

          @Override
          public Vertex next() {
            Row row = iterator.next();
            return new BlurVertex(BlurGraph.this, row.getId(), authorizations);
          }

          @Override
          public void remove() {
            throw new RuntimeException("Read only");
          }
        };
      }
    };
  }

  @Override
  public Iterable<Edge> getEdges(final Authorizations authorizations) {
    Query query = new Query();
    query.setRowQuery(true);
    query.setQuery("+<" + EDGE_FAMILY + "." + EDGE + ":" + TRUE + ">");
    final RowIterable rowIterable = new RowIterable(_client, _table, query, null);
    return new Iterable<Edge>() {

      @Override
      public Iterator<Edge> iterator() {
        final Iterator<Row> iterator = rowIterable.iterator();
        return new Iterator<Edge>() {

          @Override
          public boolean hasNext() {
            return iterator.hasNext();
          }

          @Override
          public Edge next() {
            Row row = iterator.next();
            return new BlurEdge(BlurGraph.this, row.getId(), authorizations);
          }

          @Override
          public void remove() {
            throw new RuntimeException("Read only");
          }
        };
      }
    };
  }

  @Override
  public Property createProperty(Object id, String name, Object value, Map<String, Object> metadata,
      Visibility visibility) {
    return new PropertyBase(id, name, value, metadata, visibility) {
    };
  }

}
