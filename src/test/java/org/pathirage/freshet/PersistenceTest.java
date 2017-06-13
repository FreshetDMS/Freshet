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

import io.ebean.Ebean;
import org.apache.samza.serializers.StringSerdeFactory;
import org.junit.Test;
import org.pathirage.freshet.domain.*;
import org.pathirage.freshet.domain.Topology;

import java.util.List;

import static org.junit.Assert.*;

public class PersistenceTest {
  @Test
  public void testTopologyCreation() {
    StorageSystem system = new StorageSystem();
    system.setIdentifier("kafka1");

    system.save();

    StorageSystemProperty brokers = new StorageSystemProperty("brokers", "localhost:9092");
    brokers.setSystem(system);
    brokers.save();

    StorageSystemProperty zk = new StorageSystemProperty("zk", "localhost:2181");
    zk.setSystem(system);
    zk.save();

    system.addProperty(brokers);
    system.addProperty(zk);
    system.update();



    Stream source = new Stream();
    source.setIdentifier("source1");
    source.setKeySerdeFactory(StringSerdeFactory.class.getName());
    source.setValueSerdeFactory(StringSerdeFactory.class.getName());
    source.setPartitionCount(10);
    source.setSystem(system);
    source.save();

    system.addStream(source);
    system.update();

    Stream sink = new Stream();
    sink.setIdentifier("sink1");
    sink.setKeySerdeFactory(StringSerdeFactory.class.getName());
    sink.setValueSerdeFactory(StringSerdeFactory.class.getName());
    sink.setPartitionCount(10);
    sink.setSystem(system);
    sink.save();

    system.addStream(sink);
    system.update();

    Topology t = new Topology();
    t.setName("test-topology1");
    t.addSink(sink);
    t.addSource(source);

    t.save();

    assertNotNull(t.getId());

    Ebean.find(Topology.class)
        .where().eq("name", "test-topology1")
        .findOneOrEmpty()
        .ifPresent(it -> {
          assertEquals(it.getId(), t.getId());
          assertEquals(it.getName(), t.getName());
        });
  }

  @Test
  public void testTopologyWithJobs() {
    StorageSystem system = new StorageSystem();
    system.setIdentifier("kafka2");

    system.save();

    StorageSystemProperty brokers = new StorageSystemProperty("brokers", "localhost:9092");
    brokers.setSystem(system);
    brokers.save();

    StorageSystemProperty zk = new StorageSystemProperty("zk", "localhost:2181");
    zk.setSystem(system);
    zk.save();

    system.addProperty(brokers);
    system.addProperty(zk);
    system.update();

    Stream source = new Stream();
    source.setIdentifier("source2");
    source.setKeySerdeFactory(StringSerdeFactory.class.getName());
    source.setValueSerdeFactory(StringSerdeFactory.class.getName());
    source.setPartitionCount(10);
    source.setSystem(system);
    source.save();

    system.addStream(source);
    system.update();

    Stream sink = new Stream();
    sink.setIdentifier("sink2");
    sink.setKeySerdeFactory(StringSerdeFactory.class.getName());
    sink.setValueSerdeFactory(StringSerdeFactory.class.getName());
    sink.setPartitionCount(10);
    sink.setSystem(system);
    sink.save();

    system.addStream(sink);
    system.update();

    Stream intermediate = new Stream();
    intermediate.setIdentifier("intermediate1");
    intermediate.setKeySerdeFactory(StringSerdeFactory.class.getName());
    intermediate.setValueSerdeFactory(StringSerdeFactory.class.getName());
    intermediate.setPartitionCount(10);
    intermediate.setSystem(system);
    intermediate.save();

    Stream metrics = new Stream();
    metrics.setIdentifier("metrics1");
    metrics.setKeySerdeFactory(StringSerdeFactory.class.getName());
    metrics.setValueSerdeFactory(StringSerdeFactory.class.getName());
    metrics.setPartitionCount(1);
    metrics.setSystem(system);
    metrics.save();

    Stream coordinator = new Stream();
    coordinator.setIdentifier("coordinator1");
    coordinator.setKeySerdeFactory(StringSerdeFactory.class.getName());
    coordinator.setValueSerdeFactory(StringSerdeFactory.class.getName());
    coordinator.setPartitionCount(1);
    coordinator.setSystem(system);
    coordinator.save();

    Stream changelog = new Stream();
    changelog.setIdentifier("changelog1");
    changelog.setKeySerdeFactory(StringSerdeFactory.class.getName());
    changelog.setValueSerdeFactory(StringSerdeFactory.class.getName());
    changelog.setPartitionCount(1);
    changelog.setSystem(system);
    changelog.save();

    system.addStream(intermediate);
    system.update();

    Topology t = new Topology();
    t.setName("test-topology2");
    t.addSink(sink);
    t.addSource(source);

    t.save();

    JobProperty property = new JobProperty();
    property.setName("sample.prop");
    property.setValue("samplev-value");
    property.save();

    Job job1 = new Job();
    job1.setIdentifier("job21");
    job1.addInput(source);
    job1.addOutput(intermediate);
    job1.setMetrics(metrics);
    job1.setCoordinator(coordinator);
    job1.addChangelog(changelog);
    job1.setTopology(t);
    job1.save();

    property.setJob(job1);
    property.update();

    job1.addProperty(property);
    job1.update();

    Job job2 = new Job();
    job2.setIdentifier("job22");
    job2.addInput(intermediate);
    job2.addOutput(sink);
    job2.setTopology(t);
    job2.save();

    t.addJob(job1);
    t.addJob(job2);

    t.update();

    Ebean.find(Job.class)
        .where().eq("topology", t)
        .findEach(it -> {
          assertTrue(it.getIdentifier().equals("job21") || it.getIdentifier().equals("job22"));
        });

    Ebean.find(Topology.class)
        .where().eq("name", "test-topology2")
        .findOneOrEmpty()
        .ifPresent(it -> {
          assertEquals(2, it.getJobs().size());
          assertEquals(1, it.getSources().size());
          assertEquals(1, it.getSinks().size());
        });
  }

  @Test
  public void testTopologyTraversing() {
    StorageSystem system = new StorageSystem();
    system.setIdentifier("kafka3");

    system.save();

    StorageSystemProperty brokers = new StorageSystemProperty("brokers", "localhost:9092");
    brokers.setSystem(system);
    brokers.save();

    StorageSystemProperty zk = new StorageSystemProperty("zk", "localhost:2181");
    zk.setSystem(system);
    zk.save();

    system.addProperty(brokers);
    system.addProperty(zk);
    system.update();

    Stream source = new Stream();
    source.setIdentifier("source3");
    source.setKeySerdeFactory(StringSerdeFactory.class.getName());
    source.setValueSerdeFactory(StringSerdeFactory.class.getName());
    source.setPartitionCount(10);
    source.setSystem(system);
    source.save();

    system.addStream(source);
    system.update();

    Stream sink = new Stream();
    sink.setIdentifier("sink3");
    sink.setKeySerdeFactory(StringSerdeFactory.class.getName());
    sink.setValueSerdeFactory(StringSerdeFactory.class.getName());
    sink.setPartitionCount(10);
    sink.setSystem(system);
    sink.save();

    system.addStream(sink);
    system.update();

    Stream intermediate = new Stream();
    intermediate.setIdentifier("intermediate2");
    intermediate.setKeySerdeFactory(StringSerdeFactory.class.getName());
    intermediate.setValueSerdeFactory(StringSerdeFactory.class.getName());
    intermediate.setPartitionCount(10);
    intermediate.setSystem(system);
    intermediate.save();

    Stream metrics = new Stream();
    metrics.setIdentifier("metrics2");
    metrics.setKeySerdeFactory(StringSerdeFactory.class.getName());
    metrics.setValueSerdeFactory(StringSerdeFactory.class.getName());
    metrics.setPartitionCount(1);
    metrics.setSystem(system);
    metrics.save();

    Stream coordinator = new Stream();
    coordinator.setIdentifier("coordinator2");
    coordinator.setKeySerdeFactory(StringSerdeFactory.class.getName());
    coordinator.setValueSerdeFactory(StringSerdeFactory.class.getName());
    coordinator.setPartitionCount(1);
    coordinator.setSystem(system);
    coordinator.save();

    Stream changelog = new Stream();
    changelog.setIdentifier("changelog2");
    changelog.setKeySerdeFactory(StringSerdeFactory.class.getName());
    changelog.setValueSerdeFactory(StringSerdeFactory.class.getName());
    changelog.setPartitionCount(1);
    changelog.setSystem(system);
    changelog.save();

    system.addStream(intermediate);
    system.update();

    Topology t = new Topology();
    t.setName("test-topology3");
    t.addSink(sink);
    t.addSource(source);

    t.save();

    JobProperty property = new JobProperty();
    property.setName("sample.prop");
    property.setValue("samplev-value");
    property.save();

    Job job1 = new Job();
    job1.setIdentifier("job31");
    job1.addInput(source);
    job1.addOutput(intermediate);
    job1.setMetrics(metrics);
    job1.setCoordinator(coordinator);
    job1.addChangelog(changelog);
    job1.setTopology(t);
    job1.save();

    property.setJob(job1);
    property.update();

    job1.addProperty(property);
    job1.update();

    Job job2 = new Job();
    job2.setIdentifier("job32");
    job2.addInput(intermediate);
    job2.addOutput(sink);
    job2.setTopology(t);
    job2.save();

    t.addJob(job1);
    t.addJob(job2);

    t.update();

    Ebean.find(Topology.class)
        .where().eq("name", "test-topology2")
        .findOneOrEmpty()
        .ifPresent(it -> {
          List<Job> stage = it.nextStage();
          assertEquals("job31", stage.get(0).getIdentifier());
          stage = it.nextStage();
          assertEquals("job32", stage.get(0).getIdentifier());
          assertFalse(it.hasNextStage());
        });
  }
}
