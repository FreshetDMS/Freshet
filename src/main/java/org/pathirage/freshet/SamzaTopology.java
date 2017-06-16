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

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.samza.job.StreamJobFactory;
import org.pathirage.freshet.api.Operator;
import org.pathirage.freshet.api.System;
import org.pathirage.freshet.api.Visitor;
import org.pathirage.freshet.domain.PersistenceUtils;
import org.pathirage.freshet.domain.StorageSystem;
import org.pathirage.freshet.domain.StorageSystemProperty;
import org.pathirage.freshet.domain.Stream;
import org.pathirage.freshet.grpc.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class SamzaTopology extends VisualizableTopology {

  private static Logger logger = LoggerFactory.getLogger(SamzaTopology.class);

  private final ManagedChannel channel;
  private final FreshetGrpc.FreshetBlockingStub blockingStub;

  SamzaTopology(String name, Map<String, Node> nodes, List<String> sources, List<String> sinks, System defaultSystem, Class<? extends StreamJobFactory> jobFactoryClass, String host, int port) {
    super(name, nodes, sources, sinks, defaultSystem, jobFactoryClass, host, port);

    this.channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext(true).build();
    this.blockingStub = FreshetGrpc.newBlockingStub(channel);
  }

  @Override
  public void submit() {
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

    PBTopology.Builder builder = PBTopology.newBuilder().setName(name);

    for (String sink : sinks) {
      KafkaTopic topic = (KafkaTopic) nodes.get(sink).getValue();
      builder.addOutputs(PBStreamReference.newBuilder()
          .setIdentifier(topic.getName())
          .setSystem(topic.getSystem().getName())
          .build());
    }

    for (String source : sources) {
      KafkaTopic topic = (KafkaTopic) nodes.get(source).getValue();
      builder.addOutputs(PBStreamReference.newBuilder()
          .setIdentifier(topic.getName())
          .setSystem(topic.getSystem().getName())
          .build());
    }

    // TODO: Add support for topology properties
    Map<String, Boolean> vistorState = new HashMap<>();

    for (String sink : sinks) {
      Node n = nodes.get(sink);
      PBTopologyBuilderVisitor v = new PBTopologyBuilderVisitor(builder, vistorState);
      v.visitRoot(n);
    }

    blockingStub.submitTopology(builder.build());
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

  private void persistSystems(List<System> systems) {
    for (System system : systems) {
      PBSystem.Builder systemBuilder = PBSystem.newBuilder()
          .setIdentifier(system.getName());
      for (Map.Entry<String, String> property : system.getProperties().entrySet()) {
        systemBuilder.putProperties(property.getKey(), property.getValue());
      }

      blockingStub.registerSystem(systemBuilder.build());
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

  public class StreamVisitor implements Visitor {
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
      blockingStub.registerStream(PBStream.newBuilder()
          .setIdentifier(stream.getName())
          .setSystem(stream.getSystem().getName())
          .setKeySerdeFactory(stream.getValueSerdeFactory().getName())
          .setValueSerdeFactory(stream.getValueSerdeFactory().getName())
          .setPartitionCount(stream.getPartitionCount()).build());
      // Current implementation does not validate streams stored in the server matches with the streams used in the topology
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

  public class PBTopologyBuilderVisitor implements Visitor {
    private final PBTopology.Builder builder;
    private final Map<String, Boolean> vistorState;
    private Node root;

    PBTopologyBuilderVisitor(PBTopology.Builder builder, Map<String, Boolean> vistorState) {
      this.builder = builder;
      this.vistorState = vistorState;
    }

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

          if (!vistorState.containsKey(n.getId())) {
            KafkaTopic outputTopic = (KafkaTopic) parent.getValue();

            PBJob.Builder jobBuilder = PBJob.newBuilder()
                .setTopology(name)
                .setOperator(ByteString.copyFrom(SerializationUtils.serialize((Operator) n.getValue())))
                .addOutputs(PBStreamReference.newBuilder()
                    .setIdentifier(outputTopic.getName())
                    .setSystem(outputTopic.getSystem().getName()).build());

            for (Node input : n.getInputs()) {
              if (!isSinkOrIntermediateStream(input)) {
                String errMessage = String.format("Input of an operator must be a sink or an intermediate stream. " +
                    "But found a node %s of type %s", parent.getId(), parent.getType());
                logger.error(errMessage);
                throw new IllegalStateException(errMessage);
              }

              KafkaTopic inputTopic = (KafkaTopic) parent.getValue();
              jobBuilder.addInputs(PBStreamReference.newBuilder()
                  .setIdentifier(inputTopic.getName())
                  .setSystem(inputTopic.getSystem().getName()).build());
            }

            builder.addJobs(jobBuilder.build());
            vistorState.put(n.getId(), true);
          }

          // TODO: Do we need to handle revisits? Typical topology cannot have revisits.
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
      return n.getType() == Node.Type.SINK ||
          n.getType() == Node.Type.INTERMEDIATE_STREAM;
    }
  }
}
