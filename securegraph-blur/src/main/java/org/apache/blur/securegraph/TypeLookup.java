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

import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thrift.generated.Blur.Iface;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.generated.ColumnDefinition;
import org.apache.blur.thrift.generated.Schema;

public class TypeLookup {

  private static final Log LOG = LogFactory.getLog(TypeLookup.class);

  private final Map<String, Class<?>> _type = new ConcurrentHashMap<String, Class<?>>();
  private final String _table;
  private final Iface _client;

  public TypeLookup(Iface client, String table) {
    _client = client;
    _table = table;
  }

  public Object convertToRealType(String family, String name, String value) {
    String key = getKey(family, name);
    Class<?> clazz = _type.get(key);
    if (clazz == null) {
      loadFromSchema();
    }
    clazz = _type.get(key);
    if (clazz == null) {
      throw new RuntimeException("Column [" + key + "] not found.");
    }
    return convert(value, clazz);
  }

  private void loadFromSchema() {
    try {
      Schema schema = _client.schema(_table);
      Map<String, Map<String, ColumnDefinition>> families = schema.getFamilies();
      Set<Entry<String, Map<String, ColumnDefinition>>> entrySet = families.entrySet();
      for (Entry<String, Map<String, ColumnDefinition>> e : entrySet) {
        loadFromSchema(e.getKey(), e.getValue());
      }
    } catch (BlurException e) {
      throw new RuntimeException(e);
    } catch (TException e) {
      throw new RuntimeException(e);
    }
  }

  private void loadFromSchema(String family, Map<String, ColumnDefinition> value) {
    for (Entry<String, ColumnDefinition> e : value.entrySet()) {
      String name = e.getKey();
      ColumnDefinition columnDefinition = e.getValue();
      String key = getKey(family, name);
      String fieldType = columnDefinition.getFieldType();
      if (fieldType.equals("int")) {
        _type.put(key, Integer.class);
      } else if (fieldType.equals("float")) {
        _type.put(key, Float.class);
      } else if (fieldType.equals("string")) {
        _type.put(key, String.class);
      } else if (fieldType.equals("text")) {
        _type.put(key, String.class);
      } else if (fieldType.equals("long")) {
        _type.put(key, Long.class);
      } else if (fieldType.equals("double")) {
        _type.put(key, Double.class);
      } else {
        throw new RuntimeException("Type [" + fieldType + "] not supported");
      }
    }
  }

  public void addTypeIfNeeded(String family, String name, Object value) {
    String key = getKey(family, name);
    Class<?> clazz = _type.get(key);
    if (clazz == null) {
      loadFromSchema();
    }
    clazz = _type.get(key);
    if (clazz == null) {
      clazz = value.getClass();
      addType(family, name, clazz);
      _type.put(key, clazz);
    }
  }

  private void addType(String family, String name, Class<?> clazz) {
    ColumnDefinition columnDefinition = new ColumnDefinition();
    columnDefinition.setFamily(family);
    columnDefinition.setColumnName(name);
    columnDefinition.setFieldLessIndexed(true);
    columnDefinition.setFieldType(getFieldType(clazz));
    try {
      if (!_client.addColumnDefinition(_table, columnDefinition)) {
        LOG.info("Could not add ColumnDefinition [{0}]", columnDefinition);
      }
    } catch (BlurException e) {
      throw new RuntimeException(e);
    } catch (TException e) {
      throw new RuntimeException(e);
    }
  }

  private String getFieldType(Class<?> clazz) {
    if (clazz.equals(Integer.class)) {
      return "int";
    } else if (clazz.equals(Long.class)) {
      return "long";
    } else if (clazz.equals(Float.class)) {
      return "float";
    } else if (clazz.equals(Double.class)) {
      return "double";
    } else if (clazz.equals(String.class)) {
      return "text";
    }
    throw new RuntimeException("Type [" + clazz + "] not supported");
  }

  private Object convert(String value, Class<?> clazz) {
    if (clazz.equals(Integer.class)) {
      return Integer.parseInt(value);
    } else if (clazz.equals(Long.class)) {
      return Long.parseLong(value);
    } else if (clazz.equals(Float.class)) {
      return Float.parseFloat(value);
    } else if (clazz.equals(Double.class)) {
      return Double.parseDouble(value);
    } else if (clazz.equals(String.class)) {
      return value;
    }
    throw new RuntimeException("Type [" + clazz + "] not supported");
  }

  private String getKey(String family, String name) {
    return family + "." + name;
  }

}
