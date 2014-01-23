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

import static org.apache.blur.securegraph.Constants.NAME;
import static org.apache.blur.securegraph.Constants.PROPERTY_FAMILY;
import static org.apache.blur.securegraph.Constants.VISIBILITY;

import java.util.Iterator;
import java.util.List;

import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thrift.generated.Blur.Iface;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.generated.BlurQuery;
import org.apache.blur.thrift.generated.BlurResult;
import org.apache.blur.thrift.generated.BlurResults;
import org.apache.blur.thrift.generated.Column;
import org.apache.blur.thrift.generated.FetchRecordResult;
import org.apache.blur.thrift.generated.FetchResult;
import org.apache.blur.thrift.generated.Query;
import org.apache.blur.thrift.generated.Record;
import org.apache.blur.thrift.generated.RecordMutation;
import org.apache.blur.thrift.generated.RecordMutationType;
import org.apache.blur.thrift.generated.Row;
import org.apache.blur.thrift.generated.RowMutation;
import org.apache.blur.thrift.generated.RowMutationType;
import org.apache.blur.thrift.generated.Selector;

import com.altamiracorp.securegraph.Authorizations;
import com.altamiracorp.securegraph.Element;
import com.altamiracorp.securegraph.Graph;
import com.altamiracorp.securegraph.Property;
import com.altamiracorp.securegraph.Visibility;

public class BlurElement implements Element {

  protected final BlurGraph _graph;
  protected final Iface _client;
  protected final String _rowId;
  protected final String _table;
  protected final Row _row;
  protected final Record _elementRecord;
  protected final Visibility _visibility;

  public BlurElement(BlurGraph graph, String rowId, Authorizations authorizations) {
    _graph = graph;
    _table = graph.getTable();
    _client = graph.getClient();
    _rowId = rowId;

    Selector selector = new Selector();
    selector.setRowId(rowId);
    try {
      // @TODO Use authorizations
      FetchResult fetchRow = _client.fetchRow(_table, selector);
      _row = fetchRow.getRowResult().getRow();
      _elementRecord = Util.findRecord(_row, _rowId);
      Column visibility = Util.findColumn(_elementRecord, VISIBILITY);
      if (visibility != null) {
        _visibility = new Visibility(visibility.getValue());
      } else {
        _visibility = null;
      }
    } catch (BlurException e) {
      throw new RuntimeException(e);
    } catch (TException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public final Object getId() {
    return _rowId;
  }

  @Override
  public Visibility getVisibility() {
    return _visibility;
  }

  @Override
  public final Iterable<Property> getProperties() {
    Selector selector = new Selector();
    selector.addToColumnFamiliesToFetch(PROPERTY_FAMILY);
    selector.setRowId(_rowId);
    final RecordIterable iterable = new RecordIterable(_client, _table, selector);
    return new Iterable<Property>() {

      @Override
      public Iterator<Property> iterator() {
        final Iterator<Record> iterator = iterable.iterator();
        return new Iterator<Property>() {

          @Override
          public boolean hasNext() {
            return iterator.hasNext();
          }

          @Override
          public Property next() {
            Record record = iterator.next();
            return new BlurProperty(_graph.getLookup(), record);
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
  public final Iterable<Property> getProperties(String name) {
    try {
      BlurQuery blurQuery = new BlurQuery();
      Query query = new Query();
      query.setRowQuery(false);
      query.setQuery("rowid:" + _rowId + " " + PROPERTY_FAMILY + "." + NAME + ":" + name);
      blurQuery.setQuery(query);
      blurQuery.setSelector(new Selector());
      BlurResults results = _client.query(_table, blurQuery);
      // @TODO finish paging here...
      List<BlurResult> resultsList = results.getResults();
      final Iterator<BlurResult> iterator = resultsList.iterator();
      return new Iterable<Property>() {
        @Override
        public Iterator<Property> iterator() {
          return new Iterator<Property>() {

            @Override
            public boolean hasNext() {
              return iterator.hasNext();
            }

            @Override
            public Property next() {
              BlurResult result = iterator.next();
              FetchResult fetchResult = result.getFetchResult();
              FetchRecordResult recordResult = fetchResult.getRecordResult();
              Record record = recordResult.getRecord();
              return new BlurProperty(_graph.getLookup(), record);
            }

            @Override
            public void remove() {
              throw new RuntimeException("Read only");
            }
          };
        }
      };
    } catch (BlurException e) {
      throw new RuntimeException(e);
    } catch (TException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public final Iterable<Object> getPropertyValues(String name) {
    final Iterable<Property> properties = getProperties(name);
    return new Iterable<Object>() {
      @Override
      public Iterator<Object> iterator() {
        final Iterator<Property> iterator = properties.iterator();
        return new Iterator<Object>() {
          @Override
          public boolean hasNext() {
            return iterator.hasNext();
          }

          @Override
          public Object next() {
            return iterator.next().getValue();
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
  public final Object getPropertyValue(String name, int index) {
    for (Object o : getPropertyValues(name)) {
      if (index == 0) {
        return o;
      }
      index--;
    }
    return null;
  }

  @Override
  public final void setProperties(Property... properties) {
    // will need to remove existing props then add all?????
    throw new RuntimeException("not impl");
  }

  @Override
  public final void removeProperty(String propertyId, String name) {
    RowMutation rowMutation = new RowMutation();
    rowMutation.setRowId(_rowId);
    rowMutation.setRowMutationType(RowMutationType.UPDATE_ROW);
    RecordMutation recordMutation = new RecordMutation();
    Record record = new Record();
    record.setRecordId(propertyId);
    recordMutation.setRecord(record);
    recordMutation.setRecordMutationType(RecordMutationType.DELETE_ENTIRE_RECORD);
    rowMutation.addToRecordMutations(recordMutation);
    try {
      _client.mutate(rowMutation);
    } catch (BlurException e) {
      throw new RuntimeException(e);
    } catch (TException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public final Graph getGraph() {
    return _graph;
  }

  @Override
  public String toString() {
    return "BlurElement {\n\trowId=" + _rowId + "\n\ttable=" + _table + "\n\trow=" + _row + "\n\telementRecord="
        + _elementRecord + "\n\tvisibility=" + _visibility + "\n}";
  }

}
