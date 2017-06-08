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
import org.pathirage.freshet.domain.Job;
import org.pathirage.freshet.domain.StorageSystem;
import org.pathirage.freshet.domain.Stream;
import org.pathirage.freshet.domain.Topology;

import static org.junit.Assert.*;

public class PersistenceTest {
  @Test
  public void testTopologyCreation() {
    StorageSystem system = new StorageSystem();
    system.setIdentifier("kafka1");
    system.setBrokers("localhost:9092");
    system.setZk("localhost:2181");

    system.save();

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
    system.setBrokers("localhost:9092");
    system.setZk("localhost:2181");

    system.save();

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

    system.addStream(intermediate);
    system.update();

    Topology t = new Topology();
    t.setName("test-topology2");
    t.addSink(sink);
    t.addSource(source);

    t.save();

    Job job1 = new Job();
    job1.setIdentifier("job21");
    job1.addInput(source);
    job1.addOutput(intermediate);
    job1.setTopology(t);
    job1.save();

    Job job2 = new Job();
    job2.setIdentifier("job22");
    job2.addInput(intermediate);
    job2.addOutput(sink);
    job2.setTopology(t);
    job2.save();

    t.addJob(job1);
    t.addJob(job2);

    Ebean.find(Job.class)
        .where().eq("topology", t)
        .findEach(it -> {
          assertTrue(it.getIdentifier().equals("job21") || it.getIdentifier().equals("job22"));
        });
  }
}
