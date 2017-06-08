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

import org.apache.samza.serializers.StringSerdeFactory;
import org.junit.Assert;
import org.junit.Test;
import org.pathirage.freshet.domain.StorageSystem;
import org.pathirage.freshet.domain.Stream;
import org.pathirage.freshet.domain.Topology;

public class PersistenceTest {
  @Test
  public void testTopologyCreation() {
    StorageSystem system = new StorageSystem();
    system.setIdentifier("kafka");
    system.setBrokers("localhost:9092");
    system.setZk("localhost:2181");

    system.save();

    Stream source = new Stream();
    source.setIdentifier("source");
    source.setKeySerdeFactory(StringSerdeFactory.class.getName());
    source.setValueSerdeFactory(StringSerdeFactory.class.getName());
    source.setPartitionCount(10);
    source.setSystem(system);
    source.save();

    system.addStream(source);
    system.update();

    Stream sink = new Stream();
    sink.setIdentifier("sink");
    sink.setKeySerdeFactory(StringSerdeFactory.class.getName());
    sink.setValueSerdeFactory(StringSerdeFactory.class.getName());
    sink.setPartitionCount(10);
    sink.setSystem(system);
    sink.save();

    system.addStream(sink);
    system.update();

    Topology t = new Topology();
    t.setName("test-topology");
    t.addSink(sink);
    t.addSource(source);

    t.save();

    Assert.assertNotNull(t.getId());
  }
}
