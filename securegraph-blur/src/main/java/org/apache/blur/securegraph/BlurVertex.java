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

import static org.apache.blur.securegraph.Constants.EDGE;
import static org.apache.blur.securegraph.Constants.EDGE_FAMILY;
import static org.apache.blur.securegraph.Constants.IN;
import static org.apache.blur.securegraph.Constants.LABEL;
import static org.apache.blur.securegraph.Constants.OUT;
import static org.apache.blur.securegraph.Constants.TRUE;

import java.util.Iterator;

import org.apache.blur.thrift.generated.Query;
import org.apache.blur.thrift.generated.Row;

import com.altamiracorp.securegraph.Authorizations;
import com.altamiracorp.securegraph.Direction;
import com.altamiracorp.securegraph.Edge;
import com.altamiracorp.securegraph.Vertex;
import com.altamiracorp.securegraph.query.VertexQuery;

public class BlurVertex extends BlurElement implements Vertex {

  public BlurVertex(BlurGraph graph, String vertexId, Authorizations authorizations) {
    super(graph, vertexId, authorizations);
  }

  @Override
  public VertexQuery query(Authorizations authorizations) {
    return new BlurVertexQueryBuilder(_graph, this, null, authorizations);
  }

  @Override
  public VertexQuery query(String queryString, Authorizations authorizations) {
    return new BlurVertexQueryBuilder(_graph, this, queryString, authorizations);
  }

  @Override
  public Iterable<Edge> getEdges(Direction direction, String[] labels, final Authorizations authorizations) {
    StringBuilder builder = new StringBuilder();
    builder.append("+<+").append(EDGE_FAMILY).append('.').append(EDGE).append(':').append(TRUE).append(' ');
    switch (direction) {
    case IN:
      builder.append("+").append(EDGE_FAMILY).append('.').append(IN).append(':').append(_rowId).append(' ');
      break;
    case OUT:
      builder.append("+").append(EDGE_FAMILY).append('.').append(OUT).append(':').append(_rowId).append(' ');
      break;
    default:
      throw new IllegalArgumentException();
    }

    if (labels != null && labels.length > 0) {
      builder.append('+').append(EDGE_FAMILY).append('.').append(LABEL).append(":(");
      for (String label : labels) {
        builder.append(label).append(' ');
      }
      builder.append(")");
    }
    builder.append('>');
    Query query = new Query();
    query.setRowQuery(true);
    query.setQuery(builder.toString());
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
            return new BlurEdge(_graph, row.getId(), authorizations);
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
  public Iterable<Vertex> getVertices(final Direction direction, String[] labels, final Authorizations authorizations) {
    final Iterable<Edge> edges = getEdges(direction, labels, authorizations);
    return new Iterable<Vertex>() {
      @Override
      public Iterator<Vertex> iterator() {
        final Iterator<Edge> iterator = edges.iterator();
        return new Iterator<Vertex>() {

          @Override
          public boolean hasNext() {
            return iterator.hasNext();
          }

          @Override
          public Vertex next() {
            Edge edge = iterator.next();
            return edge.getVertex(direction, authorizations);
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
  public Iterable<Edge> getEdges(Direction direction, Authorizations authorizations) {
    return getEdges(direction, new String[] {}, authorizations);
  }

  @Override
  public Iterable<Edge> getEdges(Direction direction, String label, Authorizations authorizations) {
    return getEdges(direction, new String[] { label }, authorizations);
  }

  @Override
  public Iterable<Vertex> getVertices(Direction direction, Authorizations authorizations) {
    return getVertices(direction, new String[] {}, authorizations);
  }

  @Override
  public Iterable<Vertex> getVertices(Direction direction, String label, Authorizations authorizations) {
    return getVertices(direction, new String[] { label }, authorizations);
  }
}
