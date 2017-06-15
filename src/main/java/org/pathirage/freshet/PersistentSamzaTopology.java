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
import org.pathirage.freshet.api.Visitor;
import org.pathirage.freshet.domain.PersistenceUtils;
import org.pathirage.freshet.domain.StorageSystem;
import org.pathirage.freshet.domain.StorageSystemProperty;
import org.pathirage.freshet.domain.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class PersistentSamzaTopology extends VisualizableTopology {

  private static Logger logger = LoggerFactory.getLogger(PersistentSamzaTopology.class);

  protected PersistentSamzaTopology(String name, Map<String, Node> nodes, List<String> sources, List<String> sinks, System defaultSystem, Class<? extends StreamJobFactory> jobFactoryClass) {
    super(name, nodes, sources, sinks, defaultSystem, jobFactoryClass);
  }

  @Override
  public void run() {
    persistSystems(systemsUsedInCurrentTopologyy());

    // Introduce intermediate streams
    for (String sink : sinks) {
      Node n = nodes.get(sink);

      TopologyEnrichmentVisitor tv = new TopologyEnrichmentVisitor();
      tv.visitRoot(n);
    }

    // Create and persist streams/relations (TODO: Relation support)
    for (String sink : sinks) {
      Node n = nodes.get(sink);

      StreamVisitor tv = new StreamVisitor(name);
      tv.visitRoot(n);
    }

    for (String sink : sinks) {
      Node n = nodes.get(sink);
      OperatorVisitor v = new OperatorVisitor();
      v.visitRoot(n);
    }
  }

  @Override
  public void visualize(String outputPath) {
    for (String sink : sinks) {
      Node n = nodes.get(sink);

      TopologyEnrichmentVisitor tv = new TopologyEnrichmentVisitor();
      tv.visitRoot(n);
    }
    super.visualize(outputPath);
  }


  private void persistStream(KafkaTopic pStream) {

    if (!PersistenceUtils.isStreamExists(pStream.getName(), pStream.getSystem().getName())) {
      StorageSystem storageSystem;
      try {
        storageSystem = PersistenceUtils.findSystemByName(pStream.getSystem().getName());
      } catch (PersistenceUtils.EntityNotFoundException e) {
        throw new IllegalStateException("Cannot find storage system '" + pStream.getSystem().getName() + "'.");
      }

      Stream stream = new Stream();
      stream.setIdentifier(pStream.getName());
      stream.setSystem(storageSystem);
      stream.setPartitionCount(pStream.getPartitionCount());
      stream.setValueSerdeFactory(pStream.getValueSerdeFactory().getName());
      stream.setKeySerdeFactory(pStream.getKeySerdeFactory().getName());
      stream.save();

      storageSystem.addStream(stream);
      storageSystem.update();
    }

  }

  private void persistSystems(List<System> systems) {
    for (System system : systems) {
      if (!PersistenceUtils.isSystemExists(system.getName())) {
        StorageSystem storageSystem = new StorageSystem();
        storageSystem.setIdentifier(system.getName());
        storageSystem.save();

        for (Map.Entry<String, String> property : system.getProperties().entrySet()) {
          StorageSystemProperty storageSystemProperty = new StorageSystemProperty(property.getKey(), property.getValue());
          storageSystemProperty.setSystem(storageSystem);
          storageSystemProperty.save();
          storageSystem.addProperty(storageSystemProperty);
        }

        storageSystem.update();
      }
    }
  }

  private List<System> systemsUsedInCurrentTopologyy() {
    Map<String, System> systems = new HashMap<>();

    for (String source : sources) {
      Node n = nodes.get(source);
      if (n.getType() == Node.Type.SOURCE) {
        System s = (System) n.getValue();
        if (!systems.containsKey(s.getName())) {
          systems.put(s.getName(), s);
        }
      }
    }

    for (String sink : sinks) {
      Node n = nodes.get(sink);
      if (n.getType() == Node.Type.SINK) {
        System s = (System) n.getValue();
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
      return (List<E>) collection;
    }

    return new ArrayList<>(collection);
  }

  /**
   * Modify the topology to introduce intermediate streams between operators.
   */
  class TopologyEnrichmentVisitor implements Visitor {
    private Node root;

    Node visitRoot(Node n) {
      root = n;
      visit(n, 0, null);

      return root;
    }

    @Override
    public void visit(Node n, int ordinal, Node parent) {
      n.childrenAccept(this);

      if (n.getType() == Node.Type.OPERATOR && parent.getType() == Node.Type.OPERATOR) {
        Operator current = (Operator) n.getValue();
        KafkaTopic intermediateStream = new KafkaTopic(defaultSystem,
            String.format("%s-%s", n.getId(), parent.getId()),
            n.deriveDownstreamPartitionCount(), current.getResultKeySerdeFactory(), current.getResultValueSerdeFactory());

        Node intermediateStreamNode = new Node(intermediateStream.getName(), intermediateStream, Node.Type.INTERMEDIATE_STREAM);
        intermediateStreamNode.addInput(n);

        parent.removeInput(n);
        parent.addInput(intermediateStreamNode);
      }
    }
  }

  public static class StreamVisitor implements Visitor {
    private Node root;
    private final String topologyName;

    StreamVisitor(String topologyName) {
      this.topologyName = topologyName;
    }

    Node visitRoot(Node n) {
      root = n;
      visit(n, 0, null);
      return root;
    }

    @Override
    public void visit(Node n, int ordinal, Node parent) {
      n.childrenAccept(this);
      switch (n.getType()) {
        case SINK:
        case INTERMEDIATE_STREAM:
        case SOURCE:
          createAndPersistStream((KafkaTopic) n.getValue());
          break;
        case OPERATOR:
          if (logger.isDebugEnabled()) {
            logger.debug("Ignoring operator " + n.getId() + " of topology " + topologyName);
          }
          break;
      }
    }

    private void createAndPersistStream(KafkaTopic stream) {
      // TODO: May be we should delegate this to manager web app.
      KafkaSystem system = (KafkaSystem) stream.getSystem();
      if (!system.isStreamExists(stream)) {
        system.createStream(stream);
      }

      if (!PersistenceUtils.isStreamExists(stream.getName(), system.getName())) {
        StorageSystem storageSystem;
        try {
          storageSystem = PersistenceUtils.findSystemByName(system.getName());
        } catch (PersistenceUtils.EntityNotFoundException e) {
          String errMessage = String.format("Cannot find storage system '%s' in persistent storage.", system.getName());
          logger.error(errMessage, e);
          throw new IllegalStateException(errMessage, e);
        }

        Stream persistedStream = new Stream();
        persistedStream.setIdentifier(stream.getName());
        persistedStream.setPartitionCount(stream.getPartitionCount());
        persistedStream.setSystem(storageSystem);
        persistedStream.setKeySerdeFactory(stream.getKeySerdeFactory().getName());
        persistedStream.setValueSerdeFactory(stream.getValueSerdeFactory().getName());

        persistedStream.save();

        storageSystem.addStream(persistedStream);
        storageSystem.update();
      }

      // TODO: Do we need to check whether stream in the persistent store has same properties as current stream?
      // When we do it at the manager, manager can take care of everything.
    }
  }

  /*
   * We can encapsulate most of the scheduling logic in the visitor. But we have to make sure following constraints
   * are met:
   *   - Sinks, and sources should be created before scheduling any jobs
   *   - Job output streams and input streams should be created before scheduling of any jobs
   *   - Upstream jobs must be scheduled first. But this has a caveat. When upstream job start processing before
   *     downstream jobs, failure of downstream job scheduling may cause message loss due to upstream jobs committing
   *     consumed messages. May be we should start scheduling at sinks.
   */

  public static class OperatorVisitor implements Visitor {
    private Node root;


    Node visitRoot(Node n) {
      root = n;
      visit(n, 0, null);
      return root;
    }

    @Override
    public void visit(Node n, int ordinal, Node parent) {
      switch (n.getType()) {
        case OPERATOR:
          if (!isSinkOrIntermediateStream(parent)) {
            String errMessage = String.format("Parent of an operator must be a sink or an intermediate stream. " +
                    "But found a node %s of type %s", parent.getId(), parent.getType());
            logger.error(errMessage);
            throw new IllegalStateException(errMessage);
          }

          break;
        default:
          if (logger.isDebugEnabled()) {
            logger.debug("Ignoring stream " + n.getId());
          }
          break;
      }

      n.childrenAccept(this);
    }

    private boolean isSinkOrIntermediateStream(Node n) {
      return  n.getType() == Node.Type.SINK ||
          n.getType() == Node.Type.INTERMEDIATE_STREAM;
    }

    private void deployAndPersistOperator(Node n, Node parent) {

    }
  }


}
