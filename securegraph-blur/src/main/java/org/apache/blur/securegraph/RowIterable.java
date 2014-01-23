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

import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thrift.BlurClient;
import org.apache.blur.thrift.generated.Blur.Iface;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.generated.BlurQuery;
import org.apache.blur.thrift.generated.BlurResult;
import org.apache.blur.thrift.generated.BlurResults;
import org.apache.blur.thrift.generated.FetchResult;
import org.apache.blur.thrift.generated.FetchRowResult;
import org.apache.blur.thrift.generated.Query;
import org.apache.blur.thrift.generated.Row;
import org.apache.blur.thrift.generated.Selector;

public class RowIterable implements Iterable<Row> {

  private final String _table;
  private final Iface _client;
  private final Selector _selector;
  private final Query _query;
  private final int _skip;
  private final int _fetch;

  public static void main(String[] args) throws BlurException, TException {
    Iface client = BlurClient.getClient("127.0.0.1:40010");
    // int batchSize = 1000;
    // List<RowMutation> batch = new ArrayList<RowMutation>();
    // for (int i = 0; i < 10000; i++) {
    // RowMutation rowMutation = new RowMutation();
    // rowMutation.setTable("test");
    // rowMutation.setRowId(Integer.toString(i));
    // rowMutation.setRowMutationType(RowMutationType.REPLACE_ROW);
    // Record record = new Record();
    // record.setFamily("test");
    // record.setRecordId(Integer.toString(i));
    // record.addToColumns(new Column("id", Integer.toString(i)));
    // record.addToColumns(new Column("test", "test"));
    // rowMutation.addToRecordMutations(new
    // RecordMutation(RecordMutationType.REPLACE_ENTIRE_RECORD, record));
    // batch.add(rowMutation);
    // if (batch.size() > batchSize) {
    // client.mutateBatch(batch);
    // batch.clear();
    // }
    // }
    // if (batch.size() > 0) {
    // client.mutateBatch(batch);
    // batch.clear();
    // }

    Selector selector = new Selector();
    Query query = new Query();
    query.setQuery("test.test:test");
    query.setRowQuery(true);
    RowIterable rowIterable = new RowIterable(client, "test", query, selector);
    int i = 0;
    for (Row row : rowIterable) {
      System.out.println(i + " " + row);
      i++;
    }
  }

  public RowIterable(Iface client, String table, Query query, Selector selector) {
    this(client, table, query, selector, 0, 100);
  }

  public RowIterable(Iface client, String table, Query query, Selector selector, int skip, int fetch) {
    _client = client;
    _table = table;
    _selector = selector;
    _query = query;
    _skip = skip;
    _fetch = fetch;
  }

  @Override
  public Iterator<Row> iterator() {
    return new RowIterator(_client, _table, _query, _selector, _skip, _fetch);
  }

  static class RowIterator implements Iterator<Row> {

    final String _table;
    final Iface _client;
    final Selector _selector;
    final Query _query;
    final int _fetch;
    long _position = 0;
    BlurResults _results;
    long _totalResults;

    RowIterator(Iface client, String table, Query query, Selector selector, int skip, int fetch) {
      _client = client;
      _table = table;
      if (selector != null) {
        _selector = new Selector(selector);
      } else {
        _selector = null;
      }
      _query = query;
      _position = skip;
      _fetch = fetch;
      runQuery();
    }

    private void runQuery() {
      BlurQuery bq = new BlurQuery();
      bq.setQuery(_query);
      bq.setStart(_position);
      bq.setFetch(_fetch);
      bq.setSelector(_selector);
      try {
        _results = _client.query(_table, bq);
        _totalResults = _results.getTotalResults();
      } catch (BlurException e) {
        throw new RuntimeException(e);
      } catch (TException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public boolean hasNext() {
      if (_position < _totalResults) {
        return true;
      }
      return false;
    }

    @Override
    public Row next() {
      long index = getIndex();
      if (index >= _results.getResults().size()) {
        runQuery();
        index = getIndex();
      }
      BlurResult blurResult = _results.getResults().get((int) index);
      _position++;
      FetchResult fetchResult = blurResult.getFetchResult();
      FetchRowResult rowResult = fetchResult.getRowResult();
      Row row = rowResult.getRow();
      return row;
    }

    private long getIndex() {
      return _position - _results.getQuery().getStart();
    }

    @Override
    public void remove() {
      throw new RuntimeException("Read only");
    }

  }

}
