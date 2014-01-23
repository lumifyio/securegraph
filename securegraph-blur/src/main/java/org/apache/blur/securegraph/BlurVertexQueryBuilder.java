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

import java.util.Iterator;

import com.altamiracorp.securegraph.Authorizations;
import com.altamiracorp.securegraph.Direction;
import com.altamiracorp.securegraph.Edge;
import com.altamiracorp.securegraph.Vertex;
import com.altamiracorp.securegraph.query.VertexQuery;

import static org.apache.blur.securegraph.Constants.*;

public class BlurVertexQueryBuilder extends BlurQueryBuilder implements VertexQuery {

  private String _vertexId;

  public BlurVertexQueryBuilder(BlurGraph graph, Vertex vertex, String queryString, Authorizations authorizations) {
    super(graph, queryString, authorizations);
    _vertexId = vertex.getId().toString();
  }

  @Override
  public Iterable<Vertex> vertices() {
    final Iterable<Edge> edges = edges();
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
            return iterator.next().getOtherVertex(_vertexId, _authorizations);
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
  public Iterable<Edge> edges() {
    _builder.append("+(");
    _builder.append('<').append(EDGE_FAMILY).append('.').append(IN).append(':').append(_vertexId).append("> ");
    _builder.append('<').append(EDGE_FAMILY).append('.').append(OUT).append(':').append(_vertexId).append("> ");
    _builder.append(')');
    return super.edges();
  }

  @Override
  public Iterable<Edge> edges(Direction direction) {
    switch (direction) {
    case IN:
      _builder.append("+<").append(EDGE_FAMILY).append('.').append(IN).append(':').append(_vertexId).append("> ");
      break;
    case OUT:
      _builder.append("+<").append(EDGE_FAMILY).append('.').append(OUT).append(':').append(_vertexId).append("> ");
      break;
    default:
      throw new IllegalArgumentException();
    }
    return super.edges();
  }

}
