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

import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.Map;
import scala.collection.Seq;

import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import static scala.collection.JavaConversions.asScalaBuffer;

public class KafkaAdmin {
  private static final Logger log = LoggerFactory.getLogger(KafkaAdmin.class);

  private final ZkUtils kafkaZKUtils;
  private final KafkaConsumer<String, String> consumer;

  public KafkaAdmin(String benchmark, String brokers, String zkConnect) {
    this.kafkaZKUtils = createZkUtils(zkConnect);
    this.consumer = new KafkaConsumer<String, String>(Utils.getConsumerProperties(brokers, "kafka-admin-" + benchmark));
  }

  private ZkUtils createZkUtils(String zkConnectionStr) {
    ZkConnection zkConnection = new ZkConnection(zkConnectionStr);
    ZkClient zkClient = new ZkClient(zkConnection, 30000, new ZKStringSerializer());
    return new ZkUtils(zkClient, zkConnection, false);
  }

  public boolean isPartitionCountAndReplicationFactorMatch(String topic, int partitionCount, int replicationFactor) {
    List<PartitionInfo> partitions = getTopicMetadata(topic);

    if(partitions != null && !partitions.isEmpty()) {
      log.info("Topic " + topic + " has " + partitions.size() + " partitions (expected:" + partitionCount + ") and " +
          " the replication factor is " + partitions.get(0).replicas().length + " (expected:" + replicationFactor + ").");
    } else {
      log.info("Topic " + topic + " has no partitions." );
    }

    return partitions != null && !partitions.isEmpty() && partitions.size() == partitionCount &&
        partitions.get(0).replicas().length == replicationFactor;
  }

  private List<PartitionInfo> getTopicMetadata(String topic) {
    return consumer.listTopics().get(topic);
  }

  public boolean isTopicExists(String topic) {
    return consumer.listTopics().containsKey(topic);
  }

  public void createTopic(String topic, int partitions, int replicationFactor) {
    AdminUtils.createTopic(kafkaZKUtils, topic, partitions, replicationFactor, new Properties(), RackAwareMode.Disabled$.MODULE$);
    Utils.executeUntilSuccessOrTimeout(() -> {
      return consumer.listTopics().containsKey(topic);
    }, System.currentTimeMillis(), 30 * 1000);
  }

  public void deleteTopic(String topic) {
    AdminUtils.deleteTopic(kafkaZKUtils, topic);
  }

  public int getPartitionCount(String topic) {
    Map<String, Seq<Object>> topicToPartitionMap = kafkaZKUtils.getPartitionsForTopics(asScalaBuffer(Collections.singletonList(topic)));
    if (topicToPartitionMap.get(topic).isDefined()) {
      return topicToPartitionMap.get(topic).get().size();
    }

    throw new RuntimeException("Cannot get partition count for topic " + topic);
  }

  public Set<String> listTopics(){
    return consumer.listTopics().keySet();
  }
}

