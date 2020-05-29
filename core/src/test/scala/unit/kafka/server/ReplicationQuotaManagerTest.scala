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
package kafka.server

import java.nio.file.FileStore
import java.util.Collections
import java.util.concurrent.atomic.AtomicLong

import kafka.server.QuotaType._
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.metrics.{MetricConfig, Metrics, Quota}
import org.apache.kafka.common.utils.{MockTime, Time}
import org.junit.Assert.{assertEquals, assertFalse, assertTrue}
import org.junit.{After, Test}
import org.mockito.Mockito.mock

import scala.jdk.CollectionConverters._

class ReplicationQuotaManagerTest {
  private val mockTime = new MockTime
  private val metrics = new Metrics(new MetricConfig(), Collections.emptyList(), mockTime)

  @After
  def tearDown(): Unit = {
    metrics.close()
  }

  @Test
  def shouldSetConfiguredQuotaRate(): Unit = {
    val quotaManager = new ReplicationQuotaManager(ReplicationQuotaManagerConfig(quotaBytesPerSecond = 111), metrics, LeaderReplication, mockTime)
    assertEquals(111, quotaManager.upperBound())
  }

  @Test
  def shouldThrottleAllReplicasWhenBrokerLevelConfigSet(): Unit = {
    val quota = new ReplicationQuotaManager(ReplicationQuotaManagerConfig(allReplicasThrottled = true), metrics, QuotaType.Fetch, mockTime)

    assertTrue(quota.isThrottled(tp1(1)))
    assertTrue(quota.isThrottled(tp1(2)))
    assertTrue(quota.isThrottled(tp1(3)))
    assertTrue(quota.isThrottled(tp1(4)))
    assertTrue(quota.isThrottled(tp1(400)))

    quota.markThrottled("topic1", Seq())

    assertTrue(quota.isThrottled(tp1(1)))
    assertTrue(quota.isThrottled(tp1(2)))
    assertTrue(quota.isThrottled(tp1(3)))
    assertTrue(quota.isThrottled(tp1(4)))
    assertTrue(quota.isThrottled(tp1(400)))
  }

  @Test
  def shouldThrottleOnlyDefinedReplicas(): Unit = {
    val quota = new ReplicationQuotaManager(ReplicationQuotaManagerConfig(), metrics, QuotaType.Fetch, mockTime)
    quota.markThrottled("topic1", Seq(1, 2, 3))

    assertTrue(quota.isThrottled(tp1(1)))
    assertTrue(quota.isThrottled(tp1(2)))
    assertTrue(quota.isThrottled(tp1(3)))
    assertFalse(quota.isThrottled(tp1(4)))
  }

  @Test
  def shouldExceedQuotaThenReturnBackBelowBoundAsTimePasses(): Unit = {
    val quota = new ReplicationQuotaManager(ReplicationQuotaManagerConfig(numQuotaSamples = 10, quotaWindowSizeSeconds = 1), metrics, LeaderReplication, mockTime)

    //Given
    quota.updateQuota(new Quota(100, true))

    //Quota should not be broken when we start
    assertFalse(quota.isQuotaExceeded)

    //First window is fixed, so we'll skip it
    mockTime.sleep(1000)

    //When we record up to the quota value after half a window
    mockTime.sleep(500)
    quota.record(1)

    //Then it should not break the quota
    assertFalse(quota.isQuotaExceeded)

    //When we record half the quota (half way through the window), we still should not break
    quota.record(149) //150B, 1.5s
    assertFalse(quota.isQuotaExceeded)

    //Add a byte to push over quota
    quota.record(1) //151B, 1.5s

    //Then it should break the quota
    assertEquals(151 / 1.5, rate(metrics), 0) //151B, 1.5s
    assertTrue(quota.isQuotaExceeded)

    //When we sleep for the remaining half the window
    mockTime.sleep(500) //151B, 2s

    //Then Our rate should have halved (i.e back down below the quota)
    assertFalse(quota.isQuotaExceeded)
    assertEquals(151d / 2, rate(metrics), 0.1) //151B, 2s

    //When we sleep for another half a window (now half way through second window)
    mockTime.sleep(500)
    quota.record(99) //250B, 2.5s

    //Then the rate should be exceeded again
    assertEquals(250 / 2.5, rate(metrics), 0) //250B, 2.5s
    assertFalse(quota.isQuotaExceeded)
    quota.record(1)
    assertTrue(quota.isQuotaExceeded)
    assertEquals(251 / 2.5, rate(metrics), 0)

    //Sleep for 2 more window
    mockTime.sleep(2 * 1000) //so now at 3.5s
    assertFalse(quota.isQuotaExceeded)
    assertEquals(251d / 4.5, rate(metrics), 0)
  }

  def rate(metrics: Metrics): Double = {
    val metricName = metrics.metricName("byte-rate", LeaderReplication.toString, "Tracking byte-rate for " + LeaderReplication)
    val leaderThrottledRate = metrics.metrics.asScala(metricName).metricValue.asInstanceOf[Double]
    leaderThrottledRate
  }

  @Test
  def shouldSupportWildcardThrottledReplicas(): Unit = {
    val quota = new ReplicationQuotaManager(ReplicationQuotaManagerConfig(), metrics, LeaderReplication, mockTime)

    //When
    quota.markThrottled("MyTopic", Constants.AllReplicas)

    //Then
    assertTrue(quota.isThrottled(new TopicPartition("MyTopic", 0)))
    assertFalse(quota.isThrottled(new TopicPartition("MyOtherTopic", 0)))
  }

  @Test
  def shouldSupportNoneThrottledReplicasAndOverrideBrokerThrottles(): Unit = {
    val quota = new ReplicationQuotaManager(ReplicationQuotaManagerConfig(allReplicasThrottled = true), metrics, LeaderReplication, mockTime)

    //When
    quota.markThrottled("MyTopic", Constants.NoReplicas)

    //Then
    assertFalse("Topics that are explicitly unthrottled should not be throttled", quota.isThrottled(new TopicPartition("MyTopic", 0)))
    assertTrue(quota.isThrottled(new TopicPartition("MyOtherTopic", 0)))
  }

  @Test
  def shouldSupportOverrideBrokerThrottleWhenSomeReplicasExplicitlyThrottled(): Unit = {
    val quota = new ReplicationQuotaManager(ReplicationQuotaManagerConfig(allReplicasThrottled = true), metrics, LeaderReplication, mockTime)

    //When
    val tp0 = new TopicPartition("MyTopic", 0)
    val tp1 = new TopicPartition("MyTopic", 1)
    quota.markThrottled("MyTopic", Seq(tp0.partition()))

    //Then
    assertTrue("Replicas that are explicitly throttled should be throttled", quota.isThrottled(tp0))
    assertFalse("Replicas that are not explicitly throttled (while others in the same topic are explicitly throttled) should not be throttled", quota.isThrottled(tp1))
  }

  @Test
  def shouldSupportBrokerThrottledReplicas(): Unit = {
    val quota = new ReplicationQuotaManager(ReplicationQuotaManagerConfig(), metrics, LeaderReplication, mockTime)

    //When
    quota.markBrokerThrottled()

    //Then
    assertTrue("Should have set broker replication throttle",
      quota.isThrottled(new TopicPartition("MyTopic", 0)))
    assertTrue("Should have set broker replication throttle",
      quota.isThrottled(new TopicPartition("MyOtherTopic", 0)))

    //When
    quota.removeBrokerThrottle(false)

    //Then
    assertFalse("Should have reset broker replication throttle",
      quota.isThrottled(new TopicPartition("MyTopic", 0)))
    assertFalse("Should have reset broker replication throttle",
      quota.isThrottled(new TopicPartition("MyOtherTopic", 0)))
  }

  @Test
  def shouldResetBrokerThrottledReplicas(): Unit = {
    val quota = new ReplicationQuotaManager(ReplicationQuotaManagerConfig(allReplicasThrottled = true), metrics, LeaderReplication, mockTime)

    //When
    quota.removeBrokerThrottle(false)

    //Then
    assertFalse("Should have overridden broker replication throttle",
      quota.isThrottled(new TopicPartition("MyTopic", 0)))
    assertFalse("Should have overridden broker replication throttle",
      quota.isThrottled(new TopicPartition("MyOtherTopic", 0)))

    //When
    quota.removeBrokerThrottle(true)

    //Then
    assertTrue("Should have reset broker replication throttle",
      quota.isThrottled(new TopicPartition("MyTopic", 0)))
    assertTrue("Should have reset broker replication throttle",
      quota.isThrottled(new TopicPartition("MyOtherTopic", 0)))
  }

  @Test
  def testBrokerIsThrottledOnLowDiskAvailability(): Unit = {
    val diskThreshold = DiskUsageBasedThrottlingConfig.MinDiskThresholdBytes
    val throttledLimit: Long = DiskUsageBasedThrottlingConfig.MinThroughputBytesPerSec
    val freeDiskRemaining = new AtomicLong(0L)
    val throttlingConfig = DiskUsageBasedThrottlingConfig(
      freeDiskThresholdBytes = diskThreshold,
      throttledProduceThroughput = throttledLimit,
      logDirs = Seq(mock(classOf[FileStore])),
      enableDiskBasedThrottling = true,
      diskCheckFrequencyMs = 500L,
      freeDiskThresholdBytesRecoveryFactor = 1.5)
    val throttler = new DiskUsageBasedThrottler {
      override protected[server] def diskThrottlingConfig: DiskUsageBasedThrottlingConfig = throttlingConfig

      override protected[server] def minDiskUsableBytes: Long = freeDiskRemaining.get()

      override protected def time: Time = mockTime
    }
    val config = ReplicationQuotaManagerConfig()
    val quotaManager = new ReplicationQuotaManager(config, metrics, FollowerReplication, mockTime)
    DiskUsageBasedThrottler.registerListener(quotaManager)

    // when
    freeDiskRemaining.set(diskThreshold)
    mockTime.sleep(501)
    throttler.checkAndUpdateQuotaOnDiskUsage(mockTime.milliseconds())

    // then
    assertFalse("Throttling shouldn't happen with high disk availability", quotaManager.isThrottled(tp1(0)))
    assertEquals(config.quotaBytesPerSecond, quotaManager.upperBound())

    // when
    freeDiskRemaining.set(diskThreshold - 1L)
    mockTime.sleep(501)
    throttler.checkAndUpdateQuotaOnDiskUsage(mockTime.milliseconds())

    // then
    assertTrue("Throttling should happen with low disk availability", quotaManager.isThrottled(tp1(0)))
    assertEquals(2 * throttledLimit, quotaManager.upperBound())

    // when
    freeDiskRemaining.set(diskThreshold + 1L)
    mockTime.sleep(501)
    throttler.checkAndUpdateQuotaOnDiskUsage(mockTime.milliseconds())

    // then
    assertTrue("Throttling should still continue", quotaManager.isThrottled(tp1(0)))
    assertEquals(2 * throttledLimit, quotaManager.upperBound())

    // when
    freeDiskRemaining.set((diskThreshold * 1.5).toLong)
    mockTime.sleep(501)
    throttler.checkAndUpdateQuotaOnDiskUsage(mockTime.milliseconds())

    // then
    assertFalse("Throttling should have stopped with high disk availability", quotaManager.isThrottled(tp1(0)))
    assertEquals(config.quotaBytesPerSecond, quotaManager.upperBound())

    // finally
    DiskUsageBasedThrottler.deRegisterListener(quotaManager)
  }


  private def tp1(id: Int): TopicPartition = new TopicPartition("topic1", id)
}
