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

import org.apache.samza.job.StreamJobFactory;
import org.pathirage.freshet.api.Operator;
import org.pathirage.freshet.api.System;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TopologyBuilder {

  private String name;
  private final Map<String, Node> nodes = new HashMap<>();
  private final List<String> sources = new ArrayList<>();
  private final List<String> sinks = new ArrayList<>();
  private final Class<? extends StreamJobFactory> jobFactoryClass;
  private System defaultSystem;

  public TopologyBuilder(Class<? extends StreamJobFactory> jobFactoryClass) {
    this.jobFactoryClass = jobFactoryClass;
  }

  public TopologyBuilder setName(String name) {
    this.name = name;
    return this;
  }

  public TopologyBuilder setDefaultSystem(System system) {
    this.defaultSystem = system;

    return this;
  }

  public TopologyBuilder addSource(String id, KafkaTopic stream) {
    if (nodes.containsKey(id)) {
      throw new IllegalArgumentException("Source with id " + id + " already exists in the topology.");
    }

    nodes.put(id, new Node(id, stream, Node.Type.SOURCE));
    sources.add(id);

    return this;
  }

  public TopologyBuilder addSink(String id, KafkaTopic stream, String parent) {
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
    if (defaultSystem == null) {
      throw new IllegalStateException("Default system is not set.");
    }

    return new PersistentSamzaTopology(name, nodes, sources, sinks, defaultSystem, jobFactoryClass);
  }
}
