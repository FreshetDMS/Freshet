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
import org.pathirage.freshet.api.System;
import org.pathirage.freshet.api.Visitor;

import java.util.*;

public class PersistentTopology extends VisualizableTopology {

  protected PersistentTopology(String name, Map<String, Node> nodes, List<String> sources, List<String> sinks, System defaultSystem, Class<? extends StreamJobFactory> jobFactoryClass) {
    super(name, nodes, sources, sinks, defaultSystem, jobFactoryClass);
  }

  @Override
  public void run() {
    // We can make sure sinks and sources are there in respective systems before running the visitor.
    // We can also persist systems, source and sink streams before running the visitor
    for (String sink : sinks) {
      Node n = nodes.get(sink);
      PersistentTopologyVisitor v = new PersistentTopologyVisitor();
      v.visitRoot(n);
    }
  }

  private List<System> systemsUsedInCurrentTopologyy() {
    // Get systems from sources and sinks
    // Get default system

    Map<String, System> systems = new HashMap<>();

    for (String source : sources) {
      Node n = nodes.get(source);
      if (n.getType() == Node.Type.SOURCE) {
        System s = (System)n.getValue();
        if (!systems.containsKey(s.getName())) {
          systems.put(s.getName(), s);
        }
      }
    }

    for (String sink : sinks) {
      Node n = nodes.get(sink);
      if (n.getType() == Node.Type.SINK) {
        System s = (System)n.getValue();
        if (!systems.containsKey(s.getName())) {
          systems.put(s.getName(), s);
        }
      }
    }

    if (!systems.containsKey(defaultSystem.getName())) {
      systems.put(defaultSystem.getName(), defaultSystem);
    }

    return collectionToList(systems.values());
  }

  private <E> List<E> collectionToList(Collection<E> collection) {
    if (collection instanceof List) {
      return (List<E>)collection;
    }

    return new ArrayList<>(collection);
  }

  public static class PersistentTopologyVisitor implements Visitor {
    private Node root;


    public Node visitRoot(Node n) {
      root = n;
      visit(n, 0, null);
      return root;
    }

    @Override
    public void visit(Node n, int ordinal, Node parent) {
      n.childrenAccept(this);
      switch (n.getType()) {
        case SINK:
          break;
        case SOURCE:
        case OPERATOR:
          break;
      }
    }
  }
}
