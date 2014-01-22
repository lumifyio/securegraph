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

import com.altamiracorp.securegraph.Authorizations;
import com.altamiracorp.securegraph.Element;
import com.altamiracorp.securegraph.Graph;
import com.altamiracorp.securegraph.Vertex;
import com.altamiracorp.securegraph.query.GraphQuery;
import com.altamiracorp.securegraph.query.VertexQuery;
import com.altamiracorp.securegraph.search.SearchIndex;

public class BlurSearchIndex implements SearchIndex {

  @Override
  public void addElement(Graph graph, Element element) {
    // this will probably be a do nothing method
  }

  @Override
  public void removeElement(Graph graph, Element element) {
    // this will probably be a do nothing method
  }

  @Override
  public GraphQuery queryGraph(Graph graph, String queryString, Authorizations authorizations) {
    return new BlurQueryBuilder((BlurGraph) graph, queryString, authorizations);
  }

  @Override
  public VertexQuery queryVertex(Graph graph, Vertex vertex, String queryString, Authorizations authorizations) {
    return vertex.query(queryString, authorizations);
  }

}
