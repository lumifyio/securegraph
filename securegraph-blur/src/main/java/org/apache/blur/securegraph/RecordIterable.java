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
import java.util.List;

import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thrift.BlurClient;
import org.apache.blur.thrift.generated.Blur.Iface;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.generated.FetchResult;
import org.apache.blur.thrift.generated.FetchRowResult;
import org.apache.blur.thrift.generated.Record;
import org.apache.blur.thrift.generated.Row;
import org.apache.blur.thrift.generated.Selector;

public class RecordIterable implements Iterable<Record> {

  private final String _table;
  private final Iface _client;
  private final Selector _selector;

  public static void main(String[] args) throws BlurException, TException {
    Iface client = BlurClient.getClient("127.0.0.1:40020");
    // RowMutation rowMutation = new RowMutation();
    // rowMutation.setTable("test");
    // rowMutation.setRowId("12345");
    // rowMutation.setRowMutationType(RowMutationType.REPLACE_ROW);
    // for (int i = 0; i < 10000; i++) {
    // Record record = new Record();
    // record.setFamily("test");
    // record.setRecordId(Integer.toString(i));
    // record.addToColumns(new Column("id", Integer.toString(i)));
    // rowMutation.addToRecordMutations(new
    // RecordMutation(RecordMutationType.REPLACE_ENTIRE_RECORD, record));
    // }
    // client.mutate(rowMutation);

    Selector selector = new Selector();
    selector.setRowId("12345");
    RecordIterable rowIterable = new RecordIterable(client, "test", selector);
    for (Record record : rowIterable) {
      System.out.println(record);
    }
  }

  public RecordIterable(Iface client, String table, Selector selector) {
    _client = client;
    _table = table;
    _selector = selector;
    // @TODO check for row fetching
  }

  @Override
  public Iterator<Record> iterator() {
    return new RecordIterator(_client, _table, _selector);
  }

  static class RecordIterator implements Iterator<Record> {

    final String _table;
    final Iface _client;
    final Selector _selector;
    int _position = 0;
    Row _row;
    int _totalRecords;
    FetchRowResult _rowResult;
    FetchResult _fetchRow;

    RecordIterator(Iface client, String table, Selector selector) {
      _client = client;
      _table = table;
      _selector = new Selector(selector);
      fetchRow();
    }

    private void fetchRow() {
      try {
        Selector selector = new Selector(_selector);
        if (_fetchRow == null) {
          selector.setStartRecord(0);
        } else {
          selector.setStartRecord(_position);
        }
        _fetchRow = _client.fetchRow(_table, selector);
      } catch (BlurException e) {
        throw new RuntimeException(e);
      } catch (TException e) {
        throw new RuntimeException(e);
      }
      _rowResult = _fetchRow.getRowResult();
      _row = _rowResult.getRow();
      _totalRecords = _rowResult.getTotalRecords();
    }

    @Override
    public boolean hasNext() {
      if (_position < _totalRecords) {
        return true;
      }
      return false;
    }

    @Override
    public Record next() {
      int index = getIndex();
      List<Record> records = _row.getRecords();
      if (index >= records.size()) {
        // fetch again
        fetchRow();
        index = getIndex();
        records = _row.getRecords();
      }
      Record record = records.get(index);
      _position++;
      return record;
    }

    private int getIndex() {
      return _position - _rowResult.getStartRecord();
    }

    @Override
    public void remove() {
      throw new RuntimeException("Read only");
    }

  }

}
