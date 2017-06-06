/**
 * Copyright 2017 Milinda Pathirage
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.pathirage.freshet;

import org.pathirage.freshet.api.Operator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TopologyBuilder {

  private final Map<String, Node> nodes = new HashMap<>();
  private final List<String> sources = new ArrayList<>();
  private final List<String> sinks = new ArrayList<>();

  public TopologyBuilder addSource(String id, PartitionedStream stream) {
    if (nodes.containsKey(id)) {
      throw new IllegalArgumentException("Source with id " + id + " already exists in the topology.");
    }

    nodes.put(id, new Node(id, stream, Node.Type.SOURCE));
    sources.add(id);

    return this;
  }

  public TopologyBuilder addSink(String id, PartitionedStream stream, String parent) {
    if (nodes.containsKey(id)) {
      throw new IllegalArgumentException("Sink with id " + id + " already exists in the topology.");
    }

    if (!nodes.containsKey(parent)) {
      throw new IllegalArgumentException("Cannot find parent " + parent);
    }

    Node n = new Node(id, stream, Node.Type.SINK);
    n.addInput(nodes.get(parent));

    nodes.put(id, n);
    sinks.add(id);

    return this;
  }

  public TopologyBuilder addOperator(String id, Operator operator, String... parents) {
    if (nodes.containsKey(id)) {
      throw new IllegalArgumentException("Operator with id " + id + " already exists in the topology.");
    }

    for (String parent : parents) {
      if (!nodes.containsKey(parent)) {
        throw new IllegalArgumentException("Cannot find parent " + parent);
      }
    }

    Node n = new Node(id, operator, Node.Type.OPERATOR);
    for (String parent: parents) {
      n.addInput(nodes.get(parent));
    }

    nodes.put(id, n);

    return this;
  }

  public Topology build() {
    return new Topology(nodes, sources, sinks);
  }
}
