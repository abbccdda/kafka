/*
 Copyright 2019 Confluent Inc.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.metricsreporter.utils.CCKafkaIntegrationTestHarness;
import kafka.zk.KafkaZkClient;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import scala.collection.JavaConverters;
import scala.collection.Seq;

import static com.linkedin.kafka.cruisecontrol.monitor.sampling.KafkaSampleStore.checkTopicsCreated;
import static com.linkedin.kafka.cruisecontrol.monitor.sampling.KafkaSampleStore.createConsumer;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public class KafkaSampleStoreTest extends CCKafkaIntegrationTestHarness {
  private static final String PARTITION_TOPIC = "__KafkaCruiseControlPartitionMetricSamples";
  private static final String BROKER_TOPIC = "__KafkaCruiseControlModelTrainingSamples";

  @Override
  public int clusterSize() {
    return 3;
  }

  @Before
  public void setUp() {
    super.setUp();
  }

  @After
  public void tearDown() {
    super.tearDown();
  }

  @Test
  public void testSampleStoreCreatesTopicWithCorrectConfigsWhenItDoesNotExist() {
    KafkaZkClient kafkaZkClient = KafkaCruiseControlUtils.createKafkaZkClient(zookeeper().connectionString(),
            "LoadMonitorTaskRunnerGroup",
            "LoadMonitorTaskRunnerSetup",
            false);
    try {
      Map<String, ?> configMap = new KafkaCruiseControlConfig(getConfig()).mergedConfigValues();
      KafkaSampleStore sampleStore = new KafkaSampleStore();
      sampleStore.configure(configMap);
      try {
        Map<TopicPartition, Collection<Integer>> topicPartitionAssignments = getTopicPartitionAssignments(kafkaZkClient);
        assertEquals(64, topicPartitionAssignments.size()); // both topics should have 32 partitions each
        for (Map.Entry<TopicPartition, Collection<Integer>> topicMetadata : topicPartitionAssignments.entrySet()) {
          // all partitions should have an RF of 3
          assertEquals(3, topicMetadata.getValue().size());
        }
      } finally {
        sampleStore.close();
      }
    } finally {
      KafkaCruiseControlUtils.closeKafkaZkClientWithTimeout(kafkaZkClient);
    }
  }

  private Map<String, String> getConfig() {
    Map<String, String> configMap = new HashMap<>();
    configMap.put(KafkaSampleStore.PARTITION_METRIC_SAMPLE_STORE_TOPIC_CONFIG, PARTITION_TOPIC);
    configMap.put(KafkaSampleStore.BROKER_METRIC_SAMPLE_STORE_TOPIC_CONFIG, BROKER_TOPIC);
    configMap.put(KafkaSampleStore.SKIP_SAMPLE_STORE_TOPIC_RACK_AWARENESS_CHECK_CONFIG, "true");
    configMap.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
    configMap.put(KafkaCruiseControlConfig.ZOOKEEPER_CONNECT_CONFIG, zookeeper().connectionString());
    configMap.put(KafkaCruiseControlConfig.PARTITION_METRICS_WINDOW_MS_CONFIG, "300000");
    configMap.put(KafkaCruiseControlConfig.BROKER_METRICS_WINDOW_MS_CONFIG, "30000");
    configMap.put(KafkaCruiseControlConfig.NUM_PARTITION_METRICS_WINDOWS_CONFIG, "1");
    configMap.put(KafkaCruiseControlConfig.NUM_BROKER_METRICS_WINDOWS_CONFIG, "1");
    configMap.put(KafkaCruiseControlConfig.RECONNECT_BACKOFF_MS_CONFIG, "5000");
    configMap.put(KafkaCruiseControlConfig.ZOOKEEPER_SECURITY_ENABLED_CONFIG, "false");
    configMap.put(KafkaCruiseControlConfig.DEFAULT_GOALS_CONFIG,
            "io.confluent.cruisecontrol.analyzer.goals.CrossRackMovementGoal," +
            "com.linkedin.kafka.cruisecontrol.analyzer.goals.RackAwareGoal");
    configMap.put(KafkaCruiseControlConfig.ANOMALY_DETECTION_GOALS_CONFIG,
            "io.confluent.cruisecontrol.analyzer.goals.CrossRackMovementGoal," +
            "com.linkedin.kafka.cruisecontrol.analyzer.goals.RackAwareGoal");

    return configMap;
  }

  private Map<TopicPartition, Collection<Integer>> getTopicPartitionAssignments(KafkaZkClient zkClient) {
    Set<String> topics = new HashSet<>();
    topics.add(PARTITION_TOPIC);
    topics.add(BROKER_TOPIC);
    Map<TopicPartition, Seq<Object>> intermediateMap = JavaConverters.mapAsJavaMapConverter(
            zkClient.getReplicaAssignmentForTopics(JavaConverters.asScalaSetConverter(topics).asScala().toSet())
    ).asJava();
    return intermediateMap.entrySet()
            .stream()
            .collect(
                    Collectors.toMap(Map.Entry::getKey,
                            e -> JavaConverters.asJavaCollectionConverter(
                                    e.getValue()).asJavaCollection()
                                    .stream()
                                    .map(object -> (Integer) object)
                                    .collect(Collectors.toList())

                    )
            );
  }

  @Test
  public void testCheckTopicsCreated() throws InterruptedException {
    KafkaZkClient kafkaZkClient = KafkaCruiseControlUtils.createKafkaZkClient(zookeeper().connectionString(),
            "LoadMonitorTaskRunnerGroup",
            "LoadMonitorTaskRunnerSetup",
            false);
    final int testSleepDurationMs = 2000;
    final int testMaxIteration = 10;
    try {
      Map<String, Object> configMap = new KafkaCruiseControlConfig(getConfig()).mergedConfigValues();
      // At this point, no topics should exist
      try (KafkaConsumer<byte[], byte[]> consumer = createConsumer(configMap)) {
        // First pass should fail, and instantiate the topics
        assertFalse(checkTopicsCreated(configMap, consumer));
        Set<String> topics = JavaConverters.setAsJavaSet(kafkaZkClient.getAllTopicsInCluster(false));
        assertTrue(topics.contains(PARTITION_TOPIC));
        assertTrue(topics.contains(BROKER_TOPIC));
        // Topic creation is not instantaneous; wait for completion.
        // This is rather yucky but the test doesn't control the topic creation components.
        int i = 0;
        for (; i < testMaxIteration; i++) {
          if (checkTopicsCreated(configMap, consumer)) {
            break;
          }
          Thread.sleep(testSleepDurationMs);
        }
        assertTrue("Topics not created after " + i * testSleepDurationMs + " ms",
                i < testMaxIteration);
      }
    } finally {
      KafkaCruiseControlUtils.closeKafkaZkClientWithTimeout(kafkaZkClient);
    }
  }

}
