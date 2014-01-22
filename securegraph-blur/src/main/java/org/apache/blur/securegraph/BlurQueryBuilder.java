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
import static org.apache.blur.securegraph.Constants.PROPERTY_FAMILY;
import static org.apache.blur.securegraph.Constants.TRUE;
import static org.apache.blur.securegraph.Constants.VERTEX;
import static org.apache.blur.securegraph.Constants.VERTEX_FAMILY;

import java.util.Iterator;

import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.blur.thrift.generated.Blur.Iface;
import org.apache.blur.thrift.generated.Row;

import com.altamiracorp.securegraph.Authorizations;
import com.altamiracorp.securegraph.Edge;
import com.altamiracorp.securegraph.Vertex;
import com.altamiracorp.securegraph.query.GraphQuery;
import com.altamiracorp.securegraph.query.Predicate;
import com.altamiracorp.securegraph.query.Query;

public class BlurQueryBuilder implements Query, GraphQuery {

  private static final Log LOG = LogFactory.getLog(BlurQueryBuilder.class);

  protected final StringBuilder _builder = new StringBuilder();
  protected final Iface _client;
  protected final String _table;
  protected final BlurGraph _graph;
  protected final Authorizations _authorizations;
  protected final String _queryString;

  protected int _skip = 0;
  protected int _fetch = 100;

  public BlurQueryBuilder(BlurGraph graph, String queryString, Authorizations authorizations) {
    _graph = graph;
    _client = graph.getClient();
    _table = graph.getTable();
    _authorizations = authorizations;
    _queryString = queryString;
  }

  @Override
  public Iterable<Vertex> vertices() {
    _builder.append("+<").append(VERTEX_FAMILY).append('.').append(VERTEX).append(":").append(TRUE).append("> ");
    if (_queryString != null && !_queryString.trim().isEmpty()) {
      _builder.append("+<").append(_queryString).append(">");
    }
    org.apache.blur.thrift.generated.Query query = new org.apache.blur.thrift.generated.Query();
    LOG.info("Running Query [{0}]", _builder);

    query.setQuery(_builder.toString());
    query.setRowQuery(true);
    final RowIterable rowIterable = new RowIterable(_client, _table, query, null, _skip, _fetch);
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
            return new BlurVertex(_graph, row.getId(), _authorizations);
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
    _builder.append("+<").append(EDGE_FAMILY).append('.').append(EDGE).append(":").append(TRUE).append("> ");
    if (_queryString != null && !_queryString.trim().isEmpty()) {
      _builder.append("+<").append(_queryString).append(">");
    }
    LOG.info("Running Query [{0}]", _builder);
    org.apache.blur.thrift.generated.Query query = new org.apache.blur.thrift.generated.Query();
    query.setQuery(_builder.toString());
    query.setRowQuery(true);
    final RowIterable rowIterable = new RowIterable(_client, _table, query, null, _skip, _fetch);
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
            return new BlurEdge(_graph, row.getId(), _authorizations);
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
  public <T> Query range(String propertyName, T startValue, T endValue) {
    _builder.append("+<").append(PROPERTY_FAMILY).append('.').append(propertyName).append(":[")
        .append(startValue.toString()).append(" TO ").append(endValue.toString()).append("]> ");
    return this;
  }

  @Override
  public <T> Query has(String propertyName, T value) {
    _builder.append("+<").append(PROPERTY_FAMILY).append('.').append(propertyName).append(":").append(value.toString())
        .append("> ");
    return this;
  }

  @Override
  public <T> Query has(String propertyName, Predicate predicate, T value) {
    throw new RuntimeException("not impl");
  }

  @Override
  public Query skip(int count) {
    _skip = count;
    return this;
  }

  @Override
  public Query limit(int count) {
    _fetch = count;
    return this;
  }

}
