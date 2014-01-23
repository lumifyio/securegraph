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

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import com.altamiracorp.securegraph.Direction;
import com.altamiracorp.securegraph.Edge;
import com.altamiracorp.securegraph.GraphConfiguration;
import com.altamiracorp.securegraph.Property;
import com.altamiracorp.securegraph.Vertex;
import com.altamiracorp.securegraph.id.IdGenerator;
import com.altamiracorp.securegraph.query.GraphQuery;
import com.altamiracorp.securegraph.search.SearchIndex;

public class Using {

  public static void main(String[] args) {
    Map<?, ?> config = new HashMap<String, String>();
    GraphConfiguration configuration = new GraphConfiguration(config);
    configuration.set(Constants.BLUR_CONNECTIONSTR, "127.0.0.1:40010");
    configuration.set(Constants.BLUR_TABLE, "graph2");
    IdGenerator idGenerator = new IdGenerator() {
      @Override
      public Object nextId() {
        return UUID.randomUUID().toString();
      }
    };
    SearchIndex searchIndex = new BlurSearchIndex();
    BlurGraph graph = new BlurGraph(configuration, idGenerator, searchIndex);

    for (Vertex v : graph.getVertices(null)) {
      System.out.println(v);
    }

    // Property prop1 = graph.createProperty("person_name_first", "Hannah",
    // null);
    // Property prop2 = graph.createProperty("person_name_last", "McCurry",
    // null);
    // Vertex vertex = graph.addVertex(null, prop1, prop2);
    SearchIndex index = graph.getSearchIndex();
    GraphQuery query = index.queryGraph(graph, "aaron", null);

    for (Vertex v : query.vertices()) {
      System.out.println(v);
      Iterable<Edge> edges = v.getEdges(Direction.OUT, "WIFE", null);
      for (Edge edge : edges) {
        System.out.println(edge);
      }
    }

  }

  private static Property[] getProperties(BlurGraph graph, Object value) {
    Property property = graph.createProperty(UUID.randomUUID().toString(), "prop_name", value, getMetaData(), null);
    return new Property[] { property };
  }

  private static Map<String, Object> getMetaData() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("1", "1");
    map.put("2", "2");
    return map;
  }

}
