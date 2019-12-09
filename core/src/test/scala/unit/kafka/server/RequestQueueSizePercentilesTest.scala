/*
 Copyright 2019 Confluent Inc.
 */

package kafka.server

import java.util.Collections
import java.util.concurrent.TimeUnit

import kafka.network.SocketServer
import org.apache.kafka.common.metrics.{MetricConfig, Metrics}
import org.apache.kafka.common.utils.MockTime
import org.junit.{After, Before, Test}
import org.junit.Assert.assertEquals

class RequestQueueSizePercentilesTest {

  private var time: MockTime = _
  private var metrics: Metrics = _

  @Before
  def beforeMethod(): Unit = {
    time = new MockTime
    metrics = new Metrics(new MetricConfig().timeWindow(1, TimeUnit.SECONDS), Collections.emptyList(), time)
  }

  @After
  def afterMethod(): Unit = {
    metrics.close()
  }

  @Test
  def testQueueSizePercentiles(): Unit = {
    val queueSizeSensor = metrics.sensor("RequestQueueSize")
    val queueSizePercentiles = RequestQueueSizePercentiles.createPercentiles(metrics, 500, SocketServer.DataPlaneMetricPrefix)
    queueSizeSensor.add(queueSizePercentiles)

    for (i <- 0 until 100000) {
      queueSizeSensor.record(i % 100)
    }
    assertEquals(80, RequestQueueSizePercentiles.dataPlaneQueueSize(metrics, "p80"), 1.0)
    assertEquals(90, RequestQueueSizePercentiles.dataPlaneQueueSize(metrics, "p90"), 1.0)
    assertEquals(95, RequestQueueSizePercentiles.dataPlaneQueueSize(metrics, "p95"), 1.0)
    assertEquals(99, RequestQueueSizePercentiles.dataPlaneQueueSize(metrics, "p99"), 1.0)
  }

  @Test
  def testQueueSizePercentilesUpToMax(): Unit = {
    val queueSizeSensor = metrics.sensor("RequestQueueSize")
    val queueSizePercentiles = RequestQueueSizePercentiles.createPercentiles(metrics, 500, SocketServer.DataPlaneMetricPrefix)
    queueSizeSensor.add(queueSizePercentiles)

    for (i <- 0 until 100000) {
      queueSizeSensor.record(i % 500)
    }
    assertEquals(400, RequestQueueSizePercentiles.dataPlaneQueueSize(metrics, "p80"), 1.0)
    assertEquals(450, RequestQueueSizePercentiles.dataPlaneQueueSize(metrics, "p90"), 1.0)
    assertEquals(475, RequestQueueSizePercentiles.dataPlaneQueueSize(metrics, "p95"), 1.0)
    assertEquals(495, RequestQueueSizePercentiles.dataPlaneQueueSize(metrics, "p99"), 1.0)
  }
}