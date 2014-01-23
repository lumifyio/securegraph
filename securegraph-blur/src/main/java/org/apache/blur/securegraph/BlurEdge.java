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

import static org.apache.blur.securegraph.Constants.IN;
import static org.apache.blur.securegraph.Constants.LABEL;
import static org.apache.blur.securegraph.Constants.OUT;

import org.apache.blur.thrift.generated.Column;

import com.altamiracorp.securegraph.Authorizations;
import com.altamiracorp.securegraph.Direction;
import com.altamiracorp.securegraph.Edge;
import com.altamiracorp.securegraph.Vertex;

public class BlurEdge extends BlurElement implements Edge {

  private final String _outVertex;
  private final String _inVertex;
  private final String _label;

  public BlurEdge(BlurGraph graph, String edgeId, Authorizations authorizations) {
    super(graph, edgeId, authorizations);
    Column label = Util.findColumn(_elementRecord, LABEL);
    if (label != null) {
      _label = label.getValue();
    } else {
      _label = null;
    }
    Column in = Util.findColumn(_elementRecord, IN);
    if (in != null) {
      _inVertex = in.getValue();
    } else {
      _inVertex = null;
    }
    Column out = Util.findColumn(_elementRecord, OUT);
    if (out != null) {
      _outVertex = out.getValue();
    } else {
      _outVertex = null;
    }
  }

  @Override
  public Vertex getOtherVertex(Object myVertexId, Authorizations authorizations) {
    Object otherVertexId = getOtherVertexId(myVertexId);
    return new BlurVertex(_graph, otherVertexId.toString(), authorizations);
  }

  @Override
  public String getLabel() {
    return _label;
  }

  @Override
  public Object getVertexId(Direction direction) {
    switch (direction) {
    case IN:
      return _inVertex;
    case OUT:
      return _outVertex;
    default:
      throw new IllegalArgumentException();
    }
  }

  @Override
  public Vertex getVertex(Direction direction, Authorizations authorizations) {
    switch (direction) {
    case IN:
      return new BlurVertex(_graph, _inVertex, authorizations);
    case OUT:
      return new BlurVertex(_graph, _outVertex, authorizations);
    default:
      throw new IllegalArgumentException();
    }
  }

  @Override
  public Object getOtherVertexId(Object myVertexId) {
    if (myVertexId.equals(_outVertex)) {
      return _inVertex;
    } else if (myVertexId.equals(_inVertex)) {
      return _outVertex;
    } else {
      throw new RuntimeException("Neither id [" + myVertexId + "].");
    }
  }

}
