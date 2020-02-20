/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.metrics

import java.lang.management.ManagementFactory
import java.util.Properties

import javax.management.ObjectName
import com.yammer.metrics.core.MetricPredicate
import org.junit.Test
import org.junit.Assert._
import org.scalatest.Assertions.assertThrows
import kafka.integration.KafkaServerTestHarness
import kafka.server._
import kafka.utils._

import scala.collection._
import scala.collection.JavaConverters._
import kafka.log.LogConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.metrics.JmxReporter

class MetricsTest extends KafkaServerTestHarness with Logging {
  val numNodes = 2
  val numParts = 2

  val overridingProps = new Properties
  overridingProps.put(KafkaConfig.NumPartitionsProp, numParts.toString)
  overridingProps.put(JmxReporter.BLACKLIST_CONFIG, "kafka.server:type=KafkaServer,name=ClusterId")

  def generateConfigs =
    TestUtils.createBrokerConfigs(numNodes, zkConnect).map(KafkaConfig.fromProps(_, overridingProps))

  val nMessages = 2

  @Test
  def testMetricsReporterAfterDeletingTopic(): Unit = {
    val topic = "test-topic-metric"
    createTopic(topic, 1, 1)
    adminZkClient.deleteTopic(topic)
    TestUtils.verifyTopicDeletion(zkClient, topic, 1, servers)
    assertEquals("Topic metrics exists after deleteTopic", Set.empty, topicMetricGroups(topic))
  }

  @Test
  def testBrokerTopicMetricsUnregisteredAfterDeletingTopic(): Unit = {
    val topic = "test-broker-topic-metric"
    createTopic(topic, 2, 1)
    // Produce a few messages to create the metrics
    // Don't consume messages as it may cause metrics to be re-created causing the test to fail, see KAFKA-5238
    TestUtils.generateAndProduceMessages(servers, topic, nMessages)
    assertTrue("Topic metrics don't exist", topicMetricGroups(topic).nonEmpty)
    servers.foreach(s => assertNotNull(s.brokerTopicStats.topicStats(topic)))
    adminZkClient.deleteTopic(topic)
    TestUtils.verifyTopicDeletion(zkClient, topic, 1, servers)
    assertEquals("Topic metrics exists after deleteTopic", Set.empty, topicMetricGroups(topic))
  }

  @Test
  def testClusterIdMetric(): Unit = {
    // Check if clusterId metric exists.
    val metrics = KafkaYammerMetrics.defaultRegistry.allMetrics
    assertEquals(metrics.keySet.asScala.count(_.getMBeanName == "kafka.server:type=KafkaServer,name=ClusterId"), 1)
  }

  @Test
  def testJMXFilter(): Unit = {
    // Check if cluster id metrics is not exposed in JMX
    assertTrue(ManagementFactory.getPlatformMBeanServer
                 .isRegistered(new ObjectName("kafka.controller:type=KafkaController,name=ActiveControllerCount")))
    assertFalse(ManagementFactory.getPlatformMBeanServer
                  .isRegistered(new ObjectName("kafka.server:type=KafkaServer,name=ClusterId")))
  }

  @Test
  def testUpdateJMXFilter(): Unit = {
    // verify previously exposed metrics are removed and existing matching metrics are added
    servers.foreach(server => server.kafkaYammerMetrics.reconfigure(
      Map(JmxReporter.BLACKLIST_CONFIG -> "kafka.controller:type=KafkaController,name=ActiveControllerCount").asJava
    ))
    assertFalse(ManagementFactory.getPlatformMBeanServer
                 .isRegistered(new ObjectName("kafka.controller:type=KafkaController,name=ActiveControllerCount")))
    assertTrue(ManagementFactory.getPlatformMBeanServer
                  .isRegistered(new ObjectName("kafka.server:type=KafkaServer,name=ClusterId")))
  }

  @Test
  def testGeneralBrokerTopicMetricsAreGreedilyRegistered(): Unit = {
    val topic = "test-broker-topic-metric"
    createTopic(topic, 2, 1)

    // The broker metrics for all topics should be greedily registered
    assertTrue("General topic metrics don't exist", topicMetrics(None).nonEmpty)
    assertEquals(servers.head.brokerTopicStats.allTopicsStats.metricMap.size, topicMetrics(None).size)
    // topic metrics should be lazily registered
    assertTrue("Topic metrics aren't lazily registered", topicMetricGroups(topic).isEmpty)
    TestUtils.generateAndProduceMessages(servers, topic, nMessages)
    assertTrue("Topic metrics aren't registered", topicMetricGroups(topic).nonEmpty)
  }

  @Test
  def testWindowsStyleTagNames(): Unit = {
    val path = "C:\\windows-path\\kafka-logs"
    val tags = Map("dir" -> path)
    val expectedMBeanName = Set(tags.keySet.head, ObjectName.quote(path)).mkString("=")
    val metric = KafkaMetricsGroup.metricName("test-metric", tags)
    assert(metric.getMBeanName.endsWith(expectedMBeanName))
  }

  @Test
  def testBrokerTopicMetricsBytesInOut(): Unit = {
    val topic = "test-bytes-in-out"
    val replicationBytesIn = BrokerTopicStats.ReplicationBytesInPerSec
    val replicationBytesOut = BrokerTopicStats.ReplicationBytesOutPerSec
    val bytesIn = s"${BrokerTopicStats.BytesInPerSec},topic=$topic"
    val bytesOut = s"${BrokerTopicStats.BytesOutPerSec},topic=$topic"

    val topicConfig = new Properties
    topicConfig.setProperty(LogConfig.MinInSyncReplicasProp, "2")
    createTopic(topic, 1, numNodes, topicConfig)
    // Produce a few messages to create the metrics
    TestUtils.generateAndProduceMessages(servers, topic, nMessages)

    // Check the log size for each broker so that we can distinguish between failures caused by replication issues
    // versus failures caused by the metrics
    val topicPartition = new TopicPartition(topic, 0)
    servers.foreach { server =>
      val log = server.logManager.getLog(new TopicPartition(topic, 0))
      val brokerId = server.config.brokerId
      val logSize = log.map(_.size)
      assertTrue(s"Expected broker $brokerId to have a Log for $topicPartition with positive size, actual: $logSize",
        logSize.map(_ > 0).getOrElse(false))
    }

    // Consume messages to make bytesOut tick
    TestUtils.consumeTopicRecords(servers, topic, nMessages)
    val initialReplicationBytesIn = TestUtils.meterCount(replicationBytesIn)
    val initialReplicationBytesOut = TestUtils.meterCount(replicationBytesOut)
    val initialBytesIn = TestUtils.meterCount(bytesIn)
    val initialBytesOut = TestUtils.meterCount(bytesOut)

    // BytesOut doesn't include replication, so it shouldn't have changed
    assertEquals(initialBytesOut, TestUtils.meterCount(bytesOut))

    // Produce a few messages to make the metrics tick
    TestUtils.generateAndProduceMessages(servers, topic, nMessages)

    assertTrue(TestUtils.meterCount(replicationBytesIn) > initialReplicationBytesIn)
    assertTrue(TestUtils.meterCount(replicationBytesOut) > initialReplicationBytesOut)
    assertTrue(TestUtils.meterCount(bytesIn) > initialBytesIn)

    // Consume messages to make bytesOut tick
    TestUtils.consumeTopicRecords(servers, topic, nMessages)

    assertTrue(TestUtils.meterCount(bytesOut) > initialBytesOut)
  }

  @Test
  def testBrokerTopicMetricsNoKeyCompactedTopicRecordsLogged(): Unit = {
    val topic = "test-compacted-topic-record-no-key"
    val topicConfig = new Properties
    topicConfig.setProperty(LogConfig.CleanupPolicyProp, LogConfig.Compact)
    createTopic(topic, 1, numNodes, topicConfig)
    try {
      TestUtils.produceMessages(servers, List(new ProducerRecord[Array[Byte], Array[Byte]](topic, "test".getBytes)))
      fail("Exception should have been thrown since a compacted topic cannot accept a message without keys")
    } catch {
      case _: Exception => // GOOD
    }
    // now the metric should kick in
    assertEquals(1, KafkaYammerMetrics.defaultRegistry.allMetrics.keySet.asScala.count(_.getMBeanName.endsWith(s"name=NoKeyCompactedTopicRecordsPerSec")))
    assertTrue(TestUtils.meterCount(s"name=NoKeyCompactedTopicRecordsPerSec") > 0)
  }

  @Test
  def testControllerMetrics(): Unit = {
    val metrics = KafkaYammerMetrics.defaultRegistry.allMetrics

    assertEquals(metrics.keySet.asScala.count(_.getMBeanName == "kafka.controller:type=KafkaController,name=ActiveControllerCount"), 1)
    assertEquals(metrics.keySet.asScala.count(_.getMBeanName == "kafka.controller:type=KafkaController,name=OfflinePartitionsCount"), 1)
    assertEquals(metrics.keySet.asScala.count(_.getMBeanName == "kafka.controller:type=KafkaController,name=PreferredReplicaImbalanceCount"), 1)
    assertEquals(metrics.keySet.asScala.count(_.getMBeanName == "kafka.controller:type=KafkaController,name=GlobalTopicCount"), 1)
    assertEquals(metrics.keySet.asScala.count(_.getMBeanName == "kafka.controller:type=KafkaController,name=GlobalPartitionCount"), 1)
    assertEquals(metrics.keySet.asScala.count(_.getMBeanName == "kafka.controller:type=KafkaController,name=TopicsToDeleteCount"), 1)
    assertEquals(metrics.keySet.asScala.count(_.getMBeanName == "kafka.controller:type=KafkaController,name=ReplicasToDeleteCount"), 1)
    assertEquals(metrics.keySet.asScala.count(_.getMBeanName == "kafka.controller:type=KafkaController,name=TopicsIneligibleToDeleteCount"), 1)
    assertEquals(metrics.keySet.asScala.count(_.getMBeanName == "kafka.controller:type=KafkaController,name=ReplicasIneligibleToDeleteCount"), 1)
  }

  @Test
  def testAggregateRecordMetricsInitialized(): Unit = {
    val metrics = KafkaYammerMetrics.defaultRegistry.allMetrics

    // We expect the aggregate metrics to be initialized greedily
    assertEquals(1, metrics.keySet.asScala.count(_.getMBeanName.startsWith("kafka.server:type=BrokerTopicMetrics,name=NoKeyCompactedTopicRecordsPerSec")))
    assertEquals(1, metrics.keySet.asScala.count(_.getMBeanName.startsWith("kafka.server:type=BrokerTopicMetrics,name=InvalidMagicNumberRecordsPerSec")))
    assertEquals(1, metrics.keySet.asScala.count(_.getMBeanName.startsWith("kafka.server:type=BrokerTopicMetrics,name=InvalidMessageCrcRecordsPerSec")))
    assertEquals(1, metrics.keySet.asScala.count(_.getMBeanName.startsWith("kafka.server:type=BrokerTopicMetrics,name=NonIncreasingOffsetRecordsPerSec")))
  }

  @Test
  def testBrokerTopicMetricsSegmentReads(): Unit = {
    val topic = "test-segment-reads"
    val segmentReads = BrokerTopicStats.SegmentReadsPerSec
    val segmentSpeculativePrefetches = BrokerTopicStats.SegmentSpeculativePrefetchesPerSec

    val topicConfig = new Properties
    topicConfig.setProperty(LogConfig.SegmentSpeculativePrefetchEnableProp, "true")
    topicConfig.setProperty(LogConfig.SegmentBytesProp, "128")
    createTopic(topic, 1, numNodes, topicConfig)

    // Initially should not have any recorded segment reads.
    assertEquals(0, TestUtils.meterCount(segmentReads))
    assertEquals(0, TestUtils.meterCount(segmentSpeculativePrefetches))

    // Verify per-topic metrics aren't exported.
    def testTopicMetricFails(metric: String): Unit = assertThrows[Throwable] {
      TestUtils.meterCount(s"${metric},topic=${topic}")
    }
    testTopicMetricFails(segmentReads)
    testTopicMetricFails(segmentSpeculativePrefetches)

    // Produce and consume a single record. There should still only be a single (active) segment,
    // therefore no speculative prefetches.
    TestUtils.generateAndProduceMessages(servers, topic, 1)
    TestUtils.consumeTopicRecords(servers, topic, 1)

    assertTrue(TestUtils.meterCount(segmentReads) > 0)
    assertEquals(0, TestUtils.meterCount(segmentSpeculativePrefetches))

    // Produce and consume enough messages to at least generate a second segment, and verify the
    // the resulting metrics. Note we cannot produce a batch larger than the segment size.
    (0 until 32).foreach(_ => TestUtils.generateAndProduceMessages(servers, topic, 1))
    TestUtils.consumeTopicRecords(servers, topic, 32)

    assertTrue(TestUtils.meterCount(segmentReads) > 1)
    assertTrue(TestUtils.meterCount(segmentSpeculativePrefetches) > 0)
    assertTrue(TestUtils.meterCount(segmentReads) - 1 >
      TestUtils.meterCount(segmentSpeculativePrefetches))
  }

  /**
   * Test that the metrics are created with the right name, testZooKeeperStateChangeRateMetrics
   * and testZooKeeperSessionStateMetric in ZooKeeperClientTest test the metrics behaviour.
   */
  @Test
  def testSessionExpireListenerMetrics(): Unit = {
    val metrics = KafkaYammerMetrics.defaultRegistry.allMetrics

    assertEquals(metrics.keySet.asScala.count(_.getMBeanName == "kafka.server:type=SessionExpireListener,name=SessionState"), 1)
    assertEquals(metrics.keySet.asScala.count(_.getMBeanName == "kafka.server:type=SessionExpireListener,name=ZooKeeperExpiresPerSec"), 1)
    assertEquals(metrics.keySet.asScala.count(_.getMBeanName == "kafka.server:type=SessionExpireListener,name=ZooKeeperDisconnectsPerSec"), 1)
  }

  private def topicMetrics(topic: Option[String]): Set[String] = {
    val metricNames = KafkaYammerMetrics.defaultRegistry.allMetrics().keySet.asScala.map(_.getMBeanName)
    filterByTopicMetricRegex(metricNames, topic)
  }

  private def topicMetricGroups(topic: String): Set[String] = {
    val metricGroups = KafkaYammerMetrics.defaultRegistry.groupedMetrics(MetricPredicate.ALL).keySet.asScala
    filterByTopicMetricRegex(metricGroups, Some(topic))
  }

  private def filterByTopicMetricRegex(metrics: Set[String], topic: Option[String]): Set[String] = {
    val pattern = (".*BrokerTopicMetrics.*" + topic.map(t => s"($t)$$").getOrElse("")).r.pattern
    metrics.filter(pattern.matcher(_).matches())
  }
}
