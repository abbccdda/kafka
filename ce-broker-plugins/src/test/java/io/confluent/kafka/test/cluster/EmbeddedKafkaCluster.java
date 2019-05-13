/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.kafka.test.cluster;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import kafka.api.Request;
import kafka.server.KafkaConfig$;
import kafka.server.KafkaServer;
import kafka.utils.MockTime;
import kafka.zk.EmbeddedZookeeper;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.requests.UpdateMetadataRequest.PartitionState;
import org.apache.kafka.test.TestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;

public class EmbeddedKafkaCluster {

  private static final Logger log = LoggerFactory.getLogger(EmbeddedKafkaCluster.class);
  private static final int DEFAULT_BROKER_PORT = 0; // 0 results in a random port being selected

  private final MockTime time;
  private final List<EmbeddedKafka> brokers;
  private EmbeddedZookeeper zookeeper;

  public EmbeddedKafkaCluster() {
    time = new MockTime(System.currentTimeMillis(), System.nanoTime());
    brokers = new ArrayList<>();
  }

  public void startZooKeeper() {
    log.debug("Starting a ZooKeeper instance");
    zookeeper = new EmbeddedZookeeper();
    log.debug("ZooKeeper instance is running at {}", zkConnect());
  }

  public void startBrokers(int numBrokers, Properties overrideProps) throws Exception {
    log.debug("Initiating embedded Kafka cluster startup with config {}", overrideProps);

    Properties brokerConfig = new Properties();
    brokerConfig.put(KafkaConfig$.MODULE$.ZkConnectProp(), zkConnect());
    brokerConfig.put(KafkaConfig$.MODULE$.PortProp(), DEFAULT_BROKER_PORT);
    putIfAbsent(brokerConfig, KafkaConfig$.MODULE$.OffsetsTopicReplicationFactorProp(), (short) 1);
    brokerConfig.putAll(overrideProps);
    // use delay of 0ms otherwise failed authentications never get a response due to MockTime
    putIfAbsent(brokerConfig, KafkaConfig$.MODULE$.FailedAuthenticationDelayMsProp(), 0);
    putIfAbsent(brokerConfig, KafkaConfig$.MODULE$.GroupInitialRebalanceDelayMsProp(), 0);
    putIfAbsent(brokerConfig, KafkaConfig$.MODULE$.OffsetsTopicPartitionsProp(), 5);
    putIfAbsent(brokerConfig, KafkaConfig$.MODULE$.AutoCreateTopicsEnableProp(), true);
    int brokerIdStart = Integer.parseInt(overrideProps.getOrDefault(KafkaConfig$.MODULE$.BrokerIdProp(), "0").toString());

    for (int i = 0; i < numBrokers; i++) {
      brokerConfig.put(KafkaConfig$.MODULE$.BrokerIdProp(), brokerIdStart + i);
      log.debug("Starting a Kafka instance on port {} ...",
          brokerConfig.get(KafkaConfig$.MODULE$.PortProp()));
      EmbeddedKafka broker = new EmbeddedKafka.Builder(time).addConfigs(brokerConfig).build();
      brokers.add(broker);

      log.debug("Kafka instance started: {}", broker);
    }
  }

  private void putIfAbsent(Properties brokerConfig, String propertyKey, Object propertyValue) {
    if (!brokerConfig.containsKey(propertyKey)) {
      brokerConfig.put(propertyKey, propertyValue);
    }
  }

  public void shutdown() {
    if (brokers != null) {
      for (EmbeddedKafka broker : brokers) {
        if (broker != null) {
          broker.shutdown();
        }
      }
    }
    if (zookeeper != null) {
      zookeeper.shutdown();
    }
  }

  /**
   * The ZooKeeper connection string aka `zookeeper.connect` in `hostnameOrIp:port` format. Example:
   * `127.0.0.1:2181`. <p> You can use this to e.g. tell Kafka brokers how to connect to this
   * instance. </p>
   */
  public String zkConnect() {
    return "127.0.0.1:" + zookeeper.port();
  }

  /**
   * This cluster's `bootstrap.servers` value.  Example: `127.0.0.1:9092` for the specified listener
   * <p>You can use this to tell Kafka producers how to connect to this cluster. </p>
   */
  public String bootstrapServers(String listener) {
    return brokers.stream()
        .map(broker -> broker.brokerConnect(listener))
        .collect(Collectors.joining(","));
  }

  /**
   * Bootstrap server's for the external listener
   */
  public String bootstrapServers() {
    List<String> listeners = brokers.get(0).listeners();
    if (listeners.size() > 2) {
      throw new IllegalStateException("Listener name not specified for listeners " + listeners);
    }
    String listener = listeners.get(0);
    if (listeners.size() > 1
        && brokers.get(0).kafkaServer().config().interBrokerListenerName().value().equals(listener)) {
      listener = listeners.get(1);
    }
    return bootstrapServers(listener);
  }

  public void createTopic(String topic, int partitions, int replication) {
    brokers.get(0).createTopic(topic, partitions, replication, new Properties());
    List<TopicPartition> topicPartitions = new ArrayList<>();
    for (int partition = 0; partition < partitions; partition++) {
      topicPartitions.add(new TopicPartition(topic, partition));
    }
    waitForTopicPartitions(brokers(), topicPartitions);
  }

  private static void waitForTopicPartitions(List<KafkaServer> servers,
      List<TopicPartition> partitions) {
    partitions.forEach(partition -> waitUntilMetadataIsPropagated(servers, partition));
  }

  private static void waitUntilMetadataIsPropagated(List<KafkaServer> servers, TopicPartition tp) {
    try {
      String topic = tp.topic();
      int partition = tp.partition();
      TestUtils.waitForCondition(() ->
          servers.stream().map(server -> server.dataPlaneRequestHandlerPool().apis().metadataCache())
              .allMatch(metadataCache -> {
                Option<PartitionState> partInfo = metadataCache.getPartitionInfo(topic, partition);
                if (partInfo.isEmpty()) {
                  return false;
                }
                PartitionState metadataPartitionState = partInfo.get();
                return Request.isValidBrokerId(metadataPartitionState.basePartitionState.leader);
              }), "Metadata for topic=" + topic + " partition=" + partition + " not propagated");
    } catch (InterruptedException e) {
      throw new RuntimeException("Interrupted", e);
    }
  }

  public List<EmbeddedKafka> kafkas() {
      return Collections.unmodifiableList(brokers);
  }

  public List<KafkaServer> brokers() {
    return brokers.stream().map(EmbeddedKafka::kafkaServer).collect(Collectors.toList());
  }
}
