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

import org.apache.samza.job.local.ThreadJobFactory;
import org.apache.samza.serializers.IntegerSerdeFactory;
import org.apache.samza.serializers.JsonSerdeFactory;
import org.apache.samza.serializers.SerdeFactory;
import org.apache.samza.serializers.StringSerdeFactory;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskCoordinator;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.pathirage.freshet.api.Operator;

import java.util.Map;

public class TopologyBuilderTest {

  @BeforeClass
  public static void setup() {
    SerdeResolver.INSTANCE.register(String.class, StringSerdeFactory.class);
    SerdeResolver.INSTANCE.register(Integer.class, IntegerSerdeFactory.class);
    SerdeResolver.INSTANCE.register(Map.class, JsonSerdeFactory.class);
  }

  @Test
  public void testSimpleTopology() {
    TopologyBuilder topologyBuilder = new TopologyBuilder(ThreadJobFactory.class);

    KafkaSystem kafkaSystem = new KafkaSystem("dev-kafka", "localhost:9092", "localhost:2181");

    KafkaTopic source1 = new KafkaTopic(kafkaSystem, "source1", 1, String.class, String.class);
    KafkaTopic source2 = new KafkaTopic(kafkaSystem, "source2", 1, String.class, String.class);
    KafkaTopic sink1 = new KafkaTopic(kafkaSystem, "sink1", 1, String.class, String.class);

    topologyBuilder.setDefaultSystem(kafkaSystem)
        .addSource("source1", source1)
        .addSource("source2", source2)
        .addOperator("op1", new Operator() {
          @Override
          public Class<? extends SerdeFactory> getResultKeySerdeFactory() {
            return StringSerdeFactory.class;
          }

          @Override
          public Class<? extends SerdeFactory> getResultValueSerdeFactory() {
            return StringSerdeFactory.class;
          }

          @Override
          public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) throws Exception {

          }
        }, "source1", "source2")
        .addOperator("op2", new Operator() {
          @Override
          public Class<? extends SerdeFactory> getResultKeySerdeFactory() {
            return StringSerdeFactory.class;
          }

          @Override
          public Class<? extends SerdeFactory> getResultValueSerdeFactory() {
            return StringSerdeFactory.class;
          }

          @Override
          public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) throws Exception {

          }
        }, "op1")
        .addSink("sink1", sink1, "op2");

    Topology topology = topologyBuilder.build();

    topology.visualize("test-enrichment.png");

    Assert.assertNotNull(topology);
    Assert.assertTrue(topology.getSinks().contains("sink1"));
    Assert.assertTrue(topology.getSources().contains("source1"));
  }
}
