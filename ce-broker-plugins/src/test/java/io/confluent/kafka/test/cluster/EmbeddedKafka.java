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

import io.confluent.common.EndPoint;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Function;
import java.util.stream.Collectors;
import kafka.admin.RackAwareMode;
import kafka.server.KafkaConfig;
import kafka.server.KafkaConfig$;
import kafka.server.KafkaServer;
import kafka.utils.CoreUtils;
import kafka.utils.MockTime;
import kafka.utils.TestUtils;
import kafka.zk.AdminZkClient;
import kafka.zk.KafkaZkClient;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;

/**
 * Runs an in-memory, "embedded" instance of a Kafka broker, which listens at `127.0.0.1:9092` by
 * default.
 * <p>
 * Requires a running ZooKeeper instance to connect to.
 * </p>
 */
public class EmbeddedKafka {

  private static final Logger log = LoggerFactory.getLogger(EmbeddedKafka.class);

  private static final int DEFAULT_ZK_SESSION_TIMEOUT_MS = 10 * 1000;
  private static final int DEFAULT_ZK_CONNECTION_TIMEOUT_MS = 8 * 1000;

  private final File logDir;
  private final Properties effectiveConfig;
  public final TemporaryFolder tmpFolder;
  private final KafkaServer kafka;

  /**
   * Creates and starts an embedded Kafka broker.
   *
   * @param config Broker configuration settings.  Used to modify, for example, on which port the
   *               broker should listen to.  Note that you cannot change the `log.dirs`
   *               setting currently.
   * @param time Instance of Time
   */
  private EmbeddedKafka(final Properties config, final MockTime time) throws IOException {
    tmpFolder = new TemporaryFolder();
    tmpFolder.create();
    logDir = tmpFolder.newFolder();
    final boolean loggingEnabled = true;
    effectiveConfig = brokerConfigs(config);
    final KafkaConfig kafkaConfig = new KafkaConfig(effectiveConfig, loggingEnabled);
    log.debug("Starting embedded Kafka broker (with log.dirs={} and ZK ensemble at {}) ...",
        logDir, kafkaConfig.zkConnect());
    kafka = TestUtils.createServer(kafkaConfig, time);
    log.debug("Startup of embedded Kafka broker completed: {}", this);
  }


  /**
   * Creates the configuration for starting the Kafka broker by merging default values with
   * overwrites.
   *
   * @param overrideProps Broker configuration settings that override the default config.
   */
  private Properties brokerConfigs(Properties overrideProps) throws IOException {
    final Properties brokerConfigs = new Properties();
    brokerConfigs.put(KafkaConfig$.MODULE$.BrokerIdProp(), 0);
    brokerConfigs.put(KafkaConfig$.MODULE$.HostNameProp(), "127.0.0.1");
    brokerConfigs.put(KafkaConfig$.MODULE$.PortProp(), "9092");
    brokerConfigs.put(KafkaConfig$.MODULE$.NumPartitionsProp(), 1);
    brokerConfigs.put(KafkaConfig$.MODULE$.AutoCreateTopicsEnableProp(), true);
    brokerConfigs.put(KafkaConfig$.MODULE$.MessageMaxBytesProp(), 1000000);
    brokerConfigs.put(KafkaConfig$.MODULE$.ControlledShutdownEnableProp(), true);

    brokerConfigs.putAll(overrideProps);
    brokerConfigs.setProperty(KafkaConfig$.MODULE$.LogDirProp(), logDir.getAbsolutePath());
    return brokerConfigs;
  }

  /**
   * This broker's bootstrap server value for the specified listener. Example: `127.0.0.1:9092`.
   * <p>
   * You can use this to tell Kafka producers and consumers how to connect to this instance.
   * </p>
   */
  public String brokerConnect(String listenerName) {
    return kafka.config().hostName() + ":" + kafka.boundPort(new ListenerName(listenerName));
  }

  /**
   * The ZooKeeper connection string aka `zookeeper.connect`.
   */
  public String zkConnect() {
    return kafka.config().zkConnect();
  }

  /**
   * Shutdown the broker.
   */
  public void shutdown() {
    log.debug("Shutting down embedded Kafka broker %s ...", this);
    kafka.shutdown();
    kafka.awaitShutdown();
    log.debug("Removing logs.dir at {} ...", logDir);
    List<String> logDirs = Collections.singletonList(logDir.getAbsolutePath());
    CoreUtils.delete(scala.collection.JavaConversions.asScalaBuffer(logDirs).seq());
    tmpFolder.delete();
    log.debug("Shutdown of embedded Kafka broker completed %s.", this);
  }

  /**
   * Create a Kafka topic with the given parameters.
   *
   * @param topic The name of the topic.
   * @param partitions The number of partitions for this topic.
   * @param replication The replication factor for (partitions of) this topic.
   * @param topicConfig Additional topic-level configuration settings.
   */
  public void createTopic(String topic,
      int partitions,
      int replication,
      Properties topicConfig) {
    log.debug("Creating topic { name: {}, partitions: {}, replication: {}, config: {} }",
        topic, partitions, replication, topicConfig);
    try (KafkaZkClient kafkaZkClient = createZkClient()) {
      AdminZkClient adminZkClient = new AdminZkClient(kafkaZkClient);
      adminZkClient.createTopic(topic, partitions, replication, topicConfig,
              RackAwareMode.Enforced$.MODULE$, kafkaServer().config().tierFeature());
    }
  }

  private KafkaZkClient createZkClient() {
    return KafkaZkClient.apply(zkConnect(), false, DEFAULT_ZK_SESSION_TIMEOUT_MS,
        DEFAULT_ZK_CONNECTION_TIMEOUT_MS, Integer.MAX_VALUE, Time.SYSTEM, "testMetricGroup",
        "testMetricType", Option.empty());
  }

  public KafkaServer kafkaServer() {
    return kafka;
  }

  public EndPoint endPoint() {
    Object listenerConfig = effectiveConfig.get(KafkaConfig$.MODULE$.InterBrokerListenerNameProp());
    SecurityProtocol securityProtocol = SecurityProtocol.PLAINTEXT;
    if (listenerConfig != null) {
      securityProtocol = SecurityProtocol.forName(listenerConfig.toString());
    }
    return new EndPoint(kafka.config().hostName(),
      kafka.boundPort(ListenerName.forSecurityProtocol(securityProtocol)),
      securityProtocol);
  }

  public List<String> listeners() {
    return scala.collection.JavaConversions.seqAsJavaList(kafka.config().listeners())
        .stream().map(e -> e.listenerName().value())
        .collect(Collectors.toList());
  }

  @Override
  public String toString() {
    Map<String, String> endpoints = listeners().stream()
        .collect(Collectors.toMap(Function.identity(), this::brokerConnect));
    return String.format("Kafka brokerId=%d, endpoints=%s, zkConnect=%s",
        kafka.config().brokerId(),
        Utils.mkString(endpoints, "", "", ":", ","),
        zkConnect());
  }

  public static class Builder {
    private final Properties config;
    private final MockTime time;

    public Builder(MockTime time) {
      this.config = new Properties();
      this.time = time;
    }

    public Builder addConfigs(Properties props) {
      this.config.putAll(props);
      return this;
    }

    public EmbeddedKafka build() throws IOException {
      return new EmbeddedKafka(config, time);
    }
  }
}
