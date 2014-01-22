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

import static org.apache.blur.securegraph.Constants.META;
import static org.apache.blur.securegraph.Constants.NAME;
import static org.apache.blur.securegraph.Constants.PROPERTY_FAMILY;
import static org.apache.blur.securegraph.Constants.VALUE;
import static org.apache.blur.securegraph.Constants.VISIBILITY;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.blur.thrift.generated.Column;
import org.apache.blur.thrift.generated.Record;

import com.altamiracorp.securegraph.Property;
import com.altamiracorp.securegraph.Visibility;

public class BlurProperty implements Property {

  private final Record _record;
  private final TypeLookup _lookup;

  public BlurProperty(TypeLookup lookup, Record record) {
    _record = record;
    _lookup = lookup;
  }

  @Override
  public Object getId() {
    return _record.getRecordId();
  }

  private String get(List<Column> columns, String columnName) {
    for (Column column : columns) {
      if (column.getName().equals(columnName)) {
        return column.getValue();
      }
    }
    return null;
  }

  @Override
  public String getName() {
    return get(_record.getColumns(), NAME);
  }

  @Override
  public Object getValue() {
    return _lookup.convertToRealType(PROPERTY_FAMILY, getName(), get(_record.getColumns(), VALUE));
  }

  @Override
  public Visibility getVisibility() {
    return toVisibility(get(_record.getColumns(), VISIBILITY));
  }

  @Override
  public Map<String, Object> getMetadata() {
    Map<String, Object> metaData = new HashMap<String, Object>();
    for (Column column : _record.getColumns()) {
      String key = getKey(column.getName());
      if (key != null) {
        Object value = _lookup.convertToRealType(PROPERTY_FAMILY, column.getName(), column.getValue());
        metaData.put(key, value);
      }
    }
    return metaData;
  }

  private Visibility toVisibility(String visibility) {
    return new Visibility(visibility);
  }

  private String getKey(String name) {
    if (!name.startsWith(META)) {
      return null;
    }
    return name.substring(META.length());
  }
}
