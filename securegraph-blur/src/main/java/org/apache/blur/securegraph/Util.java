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

import java.util.List;

import org.apache.blur.thrift.generated.Column;
import org.apache.blur.thrift.generated.Record;
import org.apache.blur.thrift.generated.Row;

public class Util {

  public static Record findRecord(Row row, String recordId) {
    List<Record> records = row.getRecords();
    if (records != null) {
      for (Record record : records) {
        if (record.getRecordId().equals(recordId)) {
          return record;
        }
      }
    }
    return null;
  }

  public static Column findColumn(Record record, String columnName) {
    List<Column> columns = record.getColumns();
    if (columns != null) {
      for (Column column : columns) {
        if (column.getName().equals(columnName)) {
          return column;
        }
      }
    }
    return null;
  }
  
//  private RowMutation deleteRecord(Object rowId, Object recordId, String family) {
//    Record record = new Record();
//    record.setRecordId(recordId.toString());
//    record.setFamily(family);
//    RecordMutation recordMutation = new RecordMutation();
//    recordMutation.setRecordMutationType(RecordMutationType.DELETE_ENTIRE_RECORD);
//    recordMutation.setRecord(record);
//    RowMutation rowMutation = new RowMutation();
//    rowMutation.setTable(_table);
//    rowMutation.setRowMutationType(RowMutationType.UPDATE_ROW);
//    rowMutation.setRowId(rowId.toString());
//    rowMutation.setRecordMutations(Arrays.asList(recordMutation));
//    return rowMutation;
//  }

}
