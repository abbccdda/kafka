/*
 Copyright 2018 Confluent Inc.
 */

package kafka.durability

import java.util.Collections

import kafka.utils.MockTime
import org.apache.kafka.common.{MetricName, TopicPartition}
import org.apache.kafka.common.metrics.{MetricConfig, Metrics}
import org.junit.Test
import org.junit.Assert.assertEquals

class MetricsManagerTest {
  val metrics = new Metrics(new MetricConfig(), Collections.emptyList(), new MockTime())
  val mm = MetricsManager("0", metrics)
  val tp = new TopicPartition("test", 0)
  def metricName(name: String): MetricName =
    metrics
      .metrics()
      .keySet()
      .toArray
      .filter(_.asInstanceOf[MetricName].name().equals(name))
      .head.asInstanceOf[MetricName]

  @Test
  def MetricsTest() = {
    // Size of metrics should be 2. The first metrics is for 'counter' of total metrics registered, used internally by
    // Metrics class. The second one should be "total_lost_messages".
    assertEquals("Expected metrics count is not 2", metrics.metrics().size, 2)
    assertEquals("Metrics 'total_lost_messages' should not have any tags",
      metricName("total_lost_messages").tags().size(), 0)

    // Verify that lapse count sets to correct value.
    mm.reportDurabilityLoss(15)
    assertEquals("Variable totalLostMessages not set to 15", mm.totalLostMessages, 15)

    // Make sure metrics 'total_lost_messages' is exposed as soon as it's updated.
    val name = metricName("total_lost_messages")
    assertEquals("Metrics value is not 15, same as totalLostMessages", metrics.metric(name).metricValue(), 15L)

    // Test for 'total_messages' and 'external_lost_messages' metrics. Make sure they are exposed only at the end of
    // audit runs.
    mm.resetDurabilityRunSpan(5)
    assertEquals("Total registered metrics is not 4", metrics.metrics().size, 4)
    assertEquals("Tag count for 'total_messages' metrics is not 1",
      metricName("total_messages").tags().size(), 1)
    assertEquals("Tag count for 'external_lost_messages' metrics is not 1",
      metricName("external_lost_messages").tags().size(), 1)

    // Update metrics for a TierPartition during audit run.
    mm.updateStats(tp, Stats(1000, 50))
    // Make sure the updated TierPartitions's metrics are not exposed.
    assertEquals("Metrics value for 'total_messages' should not be updated before job completion",
      metrics.metric(metricName("total_messages")).metricValue(), 0L)
    assertEquals("Metrics value for 'external_lost_messages' should not be updated before job completion",
      metrics.metric(metricName("external_lost_messages")).metricValue(), 0L)

    // Check after completion of audit span the correct metrics value is displayed.
    mm.resetDurabilityRunSpan(10)
    assertEquals("Metrics value for 'total_messages' is not 1000",
      metrics.metric(metricName("total_messages")).metricValue(), 1000L)
    assertEquals("Metrics value for 'external_lost_messages' is not 50",
      metrics.metric(metricName("external_lost_messages")).metricValue(), 50L)

    // Check calling resetDurabilityRunSpan without generating any metrics data will lead to 0 metrics value.
    mm.resetDurabilityRunSpan(15)
    // Added to be displayed after next reset.
    mm.updateStats(tp, Stats(2000, 25))
    assertEquals("Metrics value for 'total_messages' has not reset to 0",
      metrics.metric(metricName("total_messages")).metricValue(), 0L)
    assertEquals("Metrics value for 'external_lost_messages' has not reset to 0",
      metrics.metric(metricName("external_lost_messages")).metricValue(), 0L)

    // Check after completion the of audit run span the metrics displays result of latest completed runs.
    mm.resetDurabilityRunSpan(30)
    assertEquals("Metrics value for 'total_messages' is not 2000 after job completion.",
      metrics.metric(metricName("total_messages")).metricValue(), 2000L)
    assertEquals("Metrics value for 'external_lost_message' is not 25 after completion.",
      metrics.metric(metricName("external_lost_messages")).metricValue(), 25L)
  }
}
