/*
 Copyright 2019 Confluent Inc.
 */

package kafka.server

import java.net.InetAddress
import java.util.Collections
import java.util.concurrent.TimeUnit

import kafka.network.{RequestChannel, SocketServer}
import kafka.network.RequestChannel.Session
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.requests.{OffsetFetchRequest, RequestContext, RequestHeader}
import org.apache.kafka.common.utils.MockTime
import org.apache.kafka.common.security.auth.{KafkaPrincipal, SecurityProtocol}
import org.apache.kafka.common.metrics.{MetricConfig, Metrics, Quota, Sensor}
import org.apache.kafka.common.network.{ClientInformation, ListenerName}
import org.apache.kafka.common.memory.MemoryPool
import org.apache.kafka.common.metrics.stats.Value
import org.easymock.EasyMock
import org.junit.Assert.{assertEquals, assertFalse, assertNull}
import org.junit.{After, Before, Test}

import scala.jdk.CollectionConverters._

class ClientRequestQuotaManagerTest {

  private val ioThreadpoolSize = 8
  private val networkThreadpoolSize = 4
  private val ioThreadpoolCapacity = ioThreadpoolSize * 100
  private val networkThreadpoolCapacity = networkThreadpoolSize * 100
  private val maxQueueSize = 500
  private val totalCapacity = ioThreadpoolCapacity + networkThreadpoolCapacity
  private val testUser = "ANONYMOUS"
  private val testClient = "Client1"
  private val testListener = ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT)
  private val secondListener = ListenerName.forSecurityProtocol(SecurityProtocol.SASL_PLAINTEXT)

  private val testTopicPartition = new TopicPartition("test-topic", 0)
  private val testPrincipal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, testUser)

  // long backpressure check frequency is set intentionally, because each test starts at time 0
  // and assumes that broker quota limit does not get updated on maybeRecordAndGetThrottleTimeMs() path
  // tests that do not work with this assumption must recreate ClientRequestQuotaManager with a
  // different config
  private val config = ClientQuotaManagerConfig(
    quotaBytesPerSecondDefault = 500,
    backpressureConfig = BrokerBackpressureConfig(
      backpressureEnabledInConfig = true,
      backpressureCheckFrequencyMs = 10 * TimeUnit.HOURS.toMillis(1),
      tenantEndpointListenerNames = Seq(testListener.value),
      maxQueueSize = maxQueueSize,
      minBrokerRequestQuota = 110.0,
      queueSizePercentile = "p99"
    )
  )
  private val twoTenantEndpointsConfig = ClientQuotaManagerConfig(
    quotaBytesPerSecondDefault = 500,
    backpressureConfig = BrokerBackpressureConfig(
      backpressureEnabledInConfig = true,
      backpressureCheckFrequencyMs = 10 * TimeUnit.HOURS.toMillis(1),
      tenantEndpointListenerNames = Seq(testListener.value, secondListener.value)
    )
  )
  private val noBackpressureConfig = ClientQuotaManagerConfig(
    quotaBytesPerSecondDefault = 500,
    backpressureConfig = BrokerBackpressureConfig(
      backpressureEnabledInConfig = false,
      backpressureCheckFrequencyMs = 10 * TimeUnit.SECONDS.toMillis(1),
      tenantEndpointListenerNames = Seq(testListener.value)
    )
  )
  private val noTenantListenerConfig = ClientQuotaManagerConfig(
    quotaBytesPerSecondDefault = 500,
    backpressureConfig = BrokerBackpressureConfig(
      backpressureEnabledInConfig = true,
      backpressureCheckFrequencyMs = 10 * TimeUnit.SECONDS.toMillis(1)
    )
  )

  private val noQuotasConfig = ClientQuotaManagerConfig()

  private var time: MockTime = _
  private var metrics: Metrics = _
  private var ioCapSensor: Sensor = _
  private var networkCapSensor: Sensor = _
  private var activeTenantsManager: ActiveTenantsManager = _
  private var requestQuotaManager: ClientRequestQuotaManager = _

  @Before
  def beforeMethod(): Unit = {
    // recreating time object is intentional to make sure each test is consistent (because timing
    // here is important)
    time = new MockTime
    metrics = new Metrics(new MetricConfig().timeWindow(1, TimeUnit.SECONDS), Collections.emptyList(), time)
    activeTenantsManager = new ActiveTenantsManager(metrics, time, 10000)
    requestQuotaManager = new ClientRequestQuotaManager(config, metrics, time, "", None, Some(activeTenantsManager))

    // ClientRequestQuotaManager uses these sensors to get total available time on all network
    // and all IO threads
    ioCapSensor = metrics.sensor("TotalIoThreadsPercentage")
    ioCapSensor.add(ThreadUsageMetrics.ioThreadPoolCapacityMetricName(metrics), new Value())
    ioCapSensor.record(ioThreadpoolCapacity)

    networkCapSensor = metrics.sensor("TotalNetworkThreadsPercentage")
    networkCapSensor.add(ThreadUsageMetrics.networkThreadPoolCapacityMetricName(metrics, testListener.value), new Value())
    networkCapSensor.record(networkThreadpoolCapacity)
  }

  @After
  def afterMethod(): Unit = {
    requestQuotaManager.shutdown()
    metrics.close()
  }

  private def recreateRequestQuotaManagerWithoutQuotas(): Unit = {
    requestQuotaManager.shutdown()
    requestQuotaManager = new ClientRequestQuotaManager(noQuotasConfig, metrics, time, "", None, None)
  }

  private def recreateRequestQuotaManagerWithoutBackpressure(): Unit = {
    requestQuotaManager.shutdown()
    requestQuotaManager = new ClientRequestQuotaManager(
      noBackpressureConfig, metrics, time, "", None, Some(activeTenantsManager))
  }

  private def recreateRequestQuotaManagerWithoutTenantListener(): Unit = {
    requestQuotaManager.shutdown()
    requestQuotaManager = new ClientRequestQuotaManager(
      noTenantListenerConfig, metrics, time, "", None, Some(activeTenantsManager))
  }

  private def recreateRequestQuotaManagerWithTwoTenantEnpoints(): Unit = {
    requestQuotaManager.shutdown()
    requestQuotaManager = new ClientRequestQuotaManager(
      twoTenantEndpointsConfig, metrics, time, "", None, Some(activeTenantsManager))

    networkCapSensor.add(ThreadUsageMetrics.networkThreadPoolCapacityMetricName(metrics, secondListener.value), new Value())
    networkCapSensor.record(networkThreadpoolCapacity)
  }

  @Test
  def testBackpressureIsDisabledInDefaultClientQuotaManagerConfig(): Unit = {
    val defaultConfig = ClientQuotaManagerConfig()
    assertFalse(defaultConfig.backpressureConfig.backpressureEnabledInConfig)
  }

  @Test
  def testAutoTuneRequestQuotaAllAboveFairLimit(): Unit = {
    // set broker quota limit
    requestQuotaManager.nonExemptCapacitySensor.record(1200)

    requestQuotaManager.updateQuota(Some("UserA"), Some("Client1"), Some("Client1"), Some(Quota.upperBound(800)))
    requestQuotaManager.updateQuota(Some("UserB"), Some("Client2"), Some("Client2"), Some(Quota.upperBound(800)))

    // A usage (0.6) & B usage (0.6) are above the individual fair limit
    for (_ <- 0 until 10) {
      val throttleTime1 = maybeRecord("UserA", "Client1", millisToPercent(7000))
      val throttleTime2 = maybeRecord("UserB", "Client2", millisToPercent(7000))
      time.sleep(Math.max(1000, Math.max(throttleTime1, throttleTime2)))
    }
    requestQuotaManager.maybeAutoTuneQuota(activeTenantsManager, time.milliseconds())
    assertEquals(600, requestQuotaManager.dynamicQuota("UserA", "Client1").bound(), 0)
    assertEquals(600, requestQuotaManager.dynamicQuota("UserB", "Client2").bound(), 0)

    // Once we disable backpressure, dynamic quota limit should be reset to quota config
    val newBackpressureConfig = BrokerBackpressureConfig(
      backpressureEnabledInConfig = false
    )
    requestQuotaManager.updateBackpressureConfig(newBackpressureConfig)
    assertEquals(requestQuotaManager.quota("UserA", "Client1").bound(), requestQuotaManager.dynamicQuota("UserA", "Client1").bound(), 1e-8)
    assertEquals(requestQuotaManager.quota("UserB", "Client2").bound(), requestQuotaManager.dynamicQuota("UserB", "Client2").bound(), 1e-8)
  }

  @Test
  def testAutoTuneRequestQuotaAboveAndBelowFairLimit(): Unit = {
    // set broker quota limit
    requestQuotaManager.nonExemptCapacitySensor.record(1200)

    requestQuotaManager.updateQuota(Some("UserA"), Some("Client1"), Some("Client1"), Some(Quota.upperBound(800)))
    requestQuotaManager.updateQuota(Some("UserB"), Some("Client2"), Some("Client2"), Some(Quota.upperBound(800)))

    // A usage (0.4) is above & B usage (0.6) is below the individual fair limit
    for (_ <- 0 until 10) {
      val throttleTime1 = maybeRecord("UserA", "Client1", millisToPercent(7500))
      val throttleTime2 = maybeRecord("UserB", "Client2", millisToPercent(5000))
      time.sleep(Math.max(1000, Math.max(throttleTime1, throttleTime2)))
    }
    requestQuotaManager.maybeAutoTuneQuota(activeTenantsManager, time.milliseconds())
    assertEquals(700, requestQuotaManager.dynamicQuota("UserA", "Client1").bound(), 1e-8)
    assertEquals(600, requestQuotaManager.dynamicQuota("UserB", "Client2").bound(), 1e-8)
  }

  @Test
  def testAutoTuneRequestQuotaAllBelowFairLimit(): Unit = {
    // set broker quota limit
    requestQuotaManager.nonExemptCapacitySensor.record(1200)

    requestQuotaManager.updateQuota(Some("UserA"), Some("Client1"), Some("Client1"), Some(Quota.upperBound(800)))
    requestQuotaManager.updateQuota(Some("UserB"), Some("Client2"), Some("Client2"), Some(Quota.upperBound(800)))

    // A usage (0.4) & B usage (0.4) total usage is below the broker limit
    for (_ <- 0 until 10) {
      val throttleTime1 = maybeRecord("UserA", "Client1", millisToPercent(1000))
      val throttleTime2 = maybeRecord("UserB", "Client2", millisToPercent(1000))
      time.sleep(Math.max(1000, Math.max(throttleTime1, throttleTime2)))
    }
    requestQuotaManager.maybeAutoTuneQuota(activeTenantsManager, time.milliseconds())
    // if usage below broker limit, dynamic quota of each tenant is the same as quota config
    assertEquals(requestQuotaManager.quota("UserA", "Client1").bound(), requestQuotaManager.dynamicQuota("UserA", "Client1").bound(), 1e-8)
    assertEquals(requestQuotaManager.quota("UserB", "Client2").bound(), requestQuotaManager.dynamicQuota("UserB", "Client2").bound(), 1e-8)
  }

  @Test
  def testBrokerQuotaLimitIsInitiallyUnlimited(): Unit = {
    assertEquals(Double.MaxValue, requestQuotaManager.getBrokerQuotaLimit, 1.0)
  }

  @Test
  def testBrokerQuotaLimitIsUpdatedWhenBackpressureIsDisabled(): Unit = {
    recreateRequestQuotaManagerWithoutBackpressure()

    assertFalse(requestQuotaManager.backpressureEnabled)
    assertEquals(Double.MaxValue, requestQuotaManager.getBrokerQuotaLimit, 1.0)

    // ensure enough time passes for maybeRecordAndGetThrottleTimeMs to update broker quota limit
    time.sleep(noBackpressureConfig.backpressureConfig.backpressureCheckFrequencyMs + 100)

    val request = buildRequest()
    simulateTimeOnRequestHandlerThread(request, 2)
    requestQuotaManager.maybeRecordAndGetThrottleTimeMs(request, time.milliseconds)
    val expectedBrokerQuotaLimit = totalCapacity * BrokerBackpressureConfig.DefaultMaxResourceUtilization
    assertEquals(expectedBrokerQuotaLimit, requestQuotaManager.getBrokerQuotaLimit, 0.01)

    time.sleep(1) // 1 millisecond
    request.recordNetworkThreadTimeCallback.foreach(record => record(1000000))

    // broker quota limit should not be updated after just 1 millisecond
    assertEquals(expectedBrokerQuotaLimit, requestQuotaManager.getBrokerQuotaLimit, 0.01)
  }

  @Test
  def testBrokerQuotaLimitIsNotUpdatedIfTenantListenerNotConfigured(): Unit = {
    recreateRequestQuotaManagerWithoutTenantListener()

    assertFalse(requestQuotaManager.backpressureEnabled)
    assertEquals(Double.MaxValue, requestQuotaManager.getBrokerQuotaLimit, 1.0)

    // ensure enough time passes for maybeRecordAndGetThrottleTimeMs to update broker quota limit
    time.sleep(noTenantListenerConfig.backpressureConfig.backpressureCheckFrequencyMs + 100)

    val request = buildRequest()
    simulateTimeOnRequestHandlerThread(request, 2)
    requestQuotaManager.maybeRecordAndGetThrottleTimeMs(request, time.milliseconds)
    assertEquals(Double.MaxValue, requestQuotaManager.getBrokerQuotaLimit, 1.0)

    time.sleep(1) // 1 millisecond
    request.recordNetworkThreadTimeCallback.foreach(record => record(1000000))
    assertEquals(Double.MaxValue, requestQuotaManager.getBrokerQuotaLimit, 1.0)
  }

  @Test
  def testNonExemptRequestQuotasDisabledRecordsTotalThreadUsageMetricsOnly(): Unit = {
    recreateRequestQuotaManagerWithoutQuotas()

    val request = buildRequest()
    simulateTimeOnRequestHandlerThread(request, 1)
    val throttleMs = requestQuotaManager.maybeRecordAndGetThrottleTimeMs(request, time.milliseconds)
    assertEquals(0, throttleMs)

    assertIoThreadUsageMetricValue("request-io-time", Some(0.1), 0.01)
    assertIoThreadUsageMetricValue("request-non-exempt-io-time", None, 0.01)
    assertNetworkThreadUsageMetricValue(s"request-network-time", None, 0.01)
    assertNetworkThreadUsageMetricValue(s"request-non-exempt-network-time", None, 0.01)

    requestQuotaManager.updateBrokerQuotaLimit(time.milliseconds())
    assertBackpressureMetricValue("non-exempt-request-time-capacity", None, 0.01)

    time.sleep(2) // 2 milliseconds
    request.recordNetworkThreadTimeCallback.foreach(record => record(2000000))

    assertIoThreadUsageMetricValue("request-io-time", Some(0.1), 0.01)
    assertIoThreadUsageMetricValue("request-non-exempt-io-time", None, 0.01)
    assertNetworkThreadUsageMetricValue(s"request-network-time", Some(0.2), 0.01)
    assertNetworkThreadUsageMetricValue(s"request-non-exempt-network-time", None, 0.01)

    requestQuotaManager.updateBrokerQuotaLimit(time.milliseconds())
    assertBackpressureMetricValue("non-exempt-request-time-capacity", None, 0.01)
  }

  @Test
  def testExemptRequestWithQuotasEnabledRecordsTotalThreadUsageMetricsOnly(): Unit = {
    // notice that we build request which is not exempt in real life, but we record it through the
    // exempt path
    val request = buildRequest()
    simulateTimeOnRequestHandlerThread(request, 2)
    requestQuotaManager.maybeRecordExempt(request)

    assertIoThreadUsageMetricValue("request-io-time", Some(0.2),0.01)
    assertIoThreadUsageMetricValue("request-non-exempt-io-time", None, 0.01)
    assertNetworkThreadUsageMetricValue(s"request-network-time", None, 0.01)
    assertNetworkThreadUsageMetricValue(s"request-non-exempt-network-time", None, 0.01)

    requestQuotaManager.updateBrokerQuotaLimit(time.milliseconds())
    val expectedBrokerQuotaLimit = totalCapacity * BrokerBackpressureConfig.DefaultMaxResourceUtilization - 0.2
    assertBackpressureMetricValue("non-exempt-request-time-capacity", Some(expectedBrokerQuotaLimit), 0.01)

    time.sleep(1) // 1 millisecond
    request.recordNetworkThreadTimeCallback.foreach(record => record(1000000))

    assertIoThreadUsageMetricValue("request-io-time", Some(0.2), 0.01)
    assertIoThreadUsageMetricValue("request-non-exempt-io-time", None, 0.01)
    assertNetworkThreadUsageMetricValue(s"request-network-time", Some(0.1), 0.01)
    assertNetworkThreadUsageMetricValue(s"request-non-exempt-network-time", None, 0.01)

    // account for extra exempt time on network threads
    requestQuotaManager.updateBrokerQuotaLimit(time.milliseconds())
    val updatedBrokerLimit = expectedBrokerQuotaLimit - 0.1
    assertBackpressureMetricValue("non-exempt-request-time-capacity", Some(updatedBrokerLimit), 0.01)
  }

  @Test
  def testNonExemptRequestWithQuotasEnabledRecordsAllThreadUsageMetrics(): Unit = {
    val request = buildRequest()
    simulateTimeOnRequestHandlerThread(request, 2)
    val throttleMs = requestQuotaManager.maybeRecordAndGetThrottleTimeMs(request, time.milliseconds)
    assertEquals(0, throttleMs)

    assertIoThreadUsageMetricValue("request-io-time", Some(0.2), 0.01)
    assertIoThreadUsageMetricValue("request-non-exempt-io-time", Some(0.2), 0.01)
    assertNetworkThreadUsageMetricValue(s"request-network-time", None, 0.01)
    assertNetworkThreadUsageMetricValue(s"request-non-exempt-network-time", None, 0.01)

    requestQuotaManager.updateBrokerQuotaLimit(time.milliseconds())
    val expectedBrokerQuotaLimit = totalCapacity * BrokerBackpressureConfig.DefaultMaxResourceUtilization
    assertBackpressureMetricValue("non-exempt-request-time-capacity", Some(expectedBrokerQuotaLimit), 0.01)

    time.sleep(1) // 1 millisecond
    request.recordNetworkThreadTimeCallback.foreach(record => record(1000000))

    assertIoThreadUsageMetricValue("request-io-time", Some(0.2), 0.01)
    assertIoThreadUsageMetricValue("request-non-exempt-io-time", Some(0.2), 0.01)
    assertNetworkThreadUsageMetricValue(s"request-network-time", Some(0.1), 0.01)
    assertNetworkThreadUsageMetricValue(s"request-non-exempt-network-time",  Some(0.1), 0.01)

    // broker quota limit should not change since extra non-exempt network usage is tiny
    requestQuotaManager.updateBrokerQuotaLimit(time.milliseconds())
    assertBackpressureMetricValue("non-exempt-request-time-capacity", Some(expectedBrokerQuotaLimit), 0.01)
  }

  @Test
  def testBrokerQuotaLimitEnsuresIoThreadpoolIsNotOverUtilized(): Unit = {
    val request = buildRequest()

    // over-utilize IO threads
    simulateTimeOnRequestHandlerThread(request, 1000)
    for (_ <- 0 until 8) {
      val throttleMs = requestQuotaManager.maybeRecordAndGetThrottleTimeMs(request, time.milliseconds)
      // request quota is unlimited in this test and backpressure disabled, so should not throttle
      assertEquals(0, throttleMs)
    }
    val expectedIoThreadpoolUsage = ioThreadpoolSize * 100.0

    time.sleep(1) // 1 millisecond
    // 4 network threads
    for (_ <- 0 until 4) {
      request.recordNetworkThreadTimeCallback.foreach(record => record(1000000))
    }
    assertIoThreadUsageMetricValue("request-io-time", Some(expectedIoThreadpoolUsage), 1)
    // all usage came from non-exempt request
    assertIoThreadUsageMetricValue("request-non-exempt-io-time", Some(expectedIoThreadpoolUsage), 1)
    assertNetworkThreadUsageMetricValue(s"request-network-time", Some(0.4), 0.01)
    assertNetworkThreadUsageMetricValue(s"request-non-exempt-network-time", Some(0.4), 0.01)

    requestQuotaManager.updateBrokerQuotaLimit(time.milliseconds())
    val expectedLimit = ioThreadpoolCapacity * BrokerBackpressureConfig.DefaultMaxResourceUtilization + 0.4
    assertBackpressureMetricValue("non-exempt-request-time-capacity", Some(expectedLimit), 1)
  }

  @Test
  def testBrokerQuotaLimitEnsuresNetworkThreadpoolIsNotOverUtilized(): Unit = {
    val request = buildRequest()

    simulateTimeOnRequestHandlerThread(request, 10)
    for (_ <- 0 until ioThreadpoolSize) {
      val throttleMs = requestQuotaManager.maybeRecordAndGetThrottleTimeMs(request, time.milliseconds)
      // request quota is unlimited in this test and backpressure disabled, so should not throttle
      assertEquals(0, throttleMs)
    }
    // over-utilize network threads
    time.sleep(1000) // 1 second
    for (_ <- 0 until networkThreadpoolSize) {
      request.recordNetworkThreadTimeCallback.foreach(record => record(1000000000))
    }
    val expectedNetworkThreadpoolUsage = networkThreadpoolSize * 100.0

    assertIoThreadUsageMetricValue("request-io-time", Some(8.0), 0.1)
    assertIoThreadUsageMetricValue("request-non-exempt-io-time", Some(8.0), 0.1)
    assertNetworkThreadUsageMetricValue(s"request-network-time", Some(expectedNetworkThreadpoolUsage), 1)
    // all usage came from non-exempt requests
    assertNetworkThreadUsageMetricValue(s"request-non-exempt-network-time", Some(expectedNetworkThreadpoolUsage), 1)

    requestQuotaManager.updateBrokerQuotaLimit(time.milliseconds())
    val expectedLimit = networkThreadpoolCapacity * BrokerBackpressureConfig.DefaultMaxResourceUtilization + 8.0
    assertBackpressureMetricValue("non-exempt-request-time-capacity", Some(expectedLimit), 1)
  }

  @Test
  def testBrokerQuotaLimitDoesNotFallBelowMinumum(): Unit = {
    val request = buildRequest()

    // over-utilize both IO and network threads by requests exempt from throttling
    simulateTimeOnRequestHandlerThread(request, 1000)
    for (_ <- 0 until ioThreadpoolSize) {
      requestQuotaManager.maybeRecordExempt(request)
    }
    time.sleep(1000) // 1 second
    for (_ <- 0 until networkThreadpoolSize) {
      request.recordNetworkThreadTimeCallback.foreach(record => record(1000000000))
    }

    val expectedIoThreadpoolUsage = ioThreadpoolSize * 100.0
    val expectedNetworkThreadpoolUsage = networkThreadpoolSize * 100.0
    assertIoThreadUsageMetricValue("request-io-time", Some(expectedIoThreadpoolUsage), 1)
    assertIoThreadUsageMetricValue("request-non-exempt-io-time", None, 0.01)
    assertNetworkThreadUsageMetricValue(s"request-network-time", Some(expectedNetworkThreadpoolUsage), 1)
    assertNetworkThreadUsageMetricValue(s"request-non-exempt-network-time", None, 0.01)

    requestQuotaManager.updateBrokerQuotaLimit(time.milliseconds())
    val expectedLimit = totalCapacity * BrokerBackpressureConfig.DefaultMinNonExemptRequestUtilization
    assertBackpressureMetricValue("non-exempt-request-time-capacity", Some(expectedLimit), 1)
  }

  @Test
  def testNetworkThreadUsageFromNonTenantEndpointDoesNotAffectBrokerQuotaLimit(): Unit = {
    val request = buildRequest(ListenerName.forSecurityProtocol(SecurityProtocol.SASL_PLAINTEXT))

    simulateTimeOnRequestHandlerThread(request, 10)
    for (_ <- 0 until ioThreadpoolSize) {
      val throttleMs = requestQuotaManager.maybeRecordAndGetThrottleTimeMs(request, time.milliseconds)
      // request quota is unlimited in this test and backpressure disabled, so should not throttle
      assertEquals(0, throttleMs)
    }
    // over-utilize network threads
    time.sleep(1000) // 1 second
    for (_ <- 0 until networkThreadpoolSize) {
      request.recordNetworkThreadTimeCallback.foreach(record => record(1000000000))
    }

    assertIoThreadUsageMetricValue("request-io-time", Some(8.0), 0.1)
    assertIoThreadUsageMetricValue("request-non-exempt-io-time", Some(8.0), 0.1)
    // network thread usage for tenant endpoint remains zero since request came from "non-tenant" endpoint
    assertNetworkThreadUsageMetricValue(s"request-network-time", None, 1)
    assertNetworkThreadUsageMetricValue(s"request-non-exempt-network-time", None, 1)

    requestQuotaManager.updateBrokerQuotaLimit(time.milliseconds())
    val expectedLimit = totalCapacity * BrokerBackpressureConfig.DefaultMaxResourceUtilization
    assertBackpressureMetricValue("non-exempt-request-time-capacity", Some(expectedLimit), 1)
  }

  @Test
  def testUpdateAdjustedCapacityDoesNotAdjustLimitWhenQueueSizeBelowThreshold(): Unit = {
    val brokerRequestLimit = 800.0
    val queueSizeSensor = metrics.sensor("RequestQueueSize")
    val queueSizePercentiles = RequestQueueSizePercentiles.createPercentiles(metrics, maxQueueSize, SocketServer.DataPlaneMetricPrefix)
    queueSizeSensor.add(queueSizePercentiles)

    // queue size is zero at this point (since we did not simulate requests in the queue)
    val correctedLimit = requestQuotaManager.updateAdjustedCapacity(brokerRequestLimit)
    assertEquals("Expected no request limit correction during no request load",
      brokerRequestLimit, correctedLimit, 0)

    // simulate that queue size is consistently just under queue size threshold
    for (i <- 0 until 100000) {
      queueSizeSensor.record(config.backpressureConfig.queueSizeCap - 1)
    }
    val newCorrectedLimit = requestQuotaManager.updateAdjustedCapacity(brokerRequestLimit)
    assertEquals(
      "Expected no request limit correction when queue load is below the threshold",
      brokerRequestLimit, newCorrectedLimit, 0)
  }

  @Test
  def testUpdateAdjustedCapacityDecreasesLimitDuringRequestOverloadAndBacksOffDuringUnderload(): Unit = {
    val mult = 10
    val brokerRequestLimit = config.backpressureConfig.minBrokerRequestQuota + mult * BrokerBackpressureConfig.DefaultRequestQuotaAdjustment
    val queueSizeSensor = metrics.sensor("RequestQueueSize")
    val queueSizePercentiles = RequestQueueSizePercentiles.createPercentiles(metrics, maxQueueSize, SocketServer.DataPlaneMetricPrefix)
    queueSizeSensor.add(queueSizePercentiles)

    // simulate that the request queue is consistently overloaded
    for (i <- 0 until 100000) {
      queueSizeSensor.record(config.backpressureConfig.queueSizeCap + 1)
    }

    var expectedLimit = brokerRequestLimit
    for (i <- 0 until mult) {
      val correctedLimit = requestQuotaManager.updateAdjustedCapacity(brokerRequestLimit)
      expectedLimit -= BrokerBackpressureConfig.DefaultRequestQuotaAdjustment
      assertEquals("Expected request limit correction during request overload", expectedLimit, correctedLimit, 0)
    }
    // once the request limit reaches the minimum, it does not get corrected anymore
    val minCorrectedLimit = requestQuotaManager.updateAdjustedCapacity(brokerRequestLimit)
    assertEquals("Expected request limit correction during request overload", expectedLimit, minCorrectedLimit, 0)

    time.sleep(100000)
    // simulate some small load
    for (i <- 0 until 100000) {
      queueSizeSensor.record(i % 10)
    }

    for (i <- 0 until mult) {
      val correctedLimit = requestQuotaManager.updateAdjustedCapacity(brokerRequestLimit)
      expectedLimit += BrokerBackpressureConfig.DefaultRequestQuotaAdjustment
      assertEquals("Expected request limit correction during request overload", expectedLimit, correctedLimit, 0)
    }
    // once the corrected limit reaches given 'brokerRequestLimit', it does not increase anymore
    val maxCorrectedLimit = requestQuotaManager.updateAdjustedCapacity(brokerRequestLimit)
    assertEquals("Expected request limit correction during request overload", expectedLimit, maxCorrectedLimit, 0)
  }

  @Test
  def testBrokerRequestLimitIsAdjustedOnRequestOverload(): Unit = {
    val request = buildRequest()
    simulateTimeOnRequestHandlerThread(request, 500)
    val throttleMs = requestQuotaManager.maybeRecordAndGetThrottleTimeMs(request, time.milliseconds)
    assertEquals(0, throttleMs)

    assertIoThreadUsageMetricValue("request-io-time", Some(50), 0.01)
    assertIoThreadUsageMetricValue("request-non-exempt-io-time", Some(50), 0.01)

    requestQuotaManager.updateBrokerQuotaLimit(time.milliseconds())
    val expectedBrokerQuotaLimit = totalCapacity * BrokerBackpressureConfig.DefaultMaxResourceUtilization
    assertBackpressureMetricValue("non-exempt-request-time-capacity", Some(expectedBrokerQuotaLimit), 0.01)

    time.sleep(10) // 10 millisecond
    request.recordNetworkThreadTimeCallback.foreach(record => record(10000000))
    assertNetworkThreadUsageMetricValue(s"request-network-time", Some(1), 0.01)
    assertNetworkThreadUsageMetricValue(s"request-non-exempt-network-time",  Some(1), 0.01)

    // broker quota limit should not change since extra non-exempt network usage is tiny
    requestQuotaManager.updateBrokerQuotaLimit(time.milliseconds())
    assertBackpressureMetricValue("non-exempt-request-time-capacity", Some(expectedBrokerQuotaLimit), 0.01)

    // simulate request overload
    val queueSizeSensor = metrics.sensor("RequestQueueSize")
    val queueSizePercentiles = RequestQueueSizePercentiles.createPercentiles(metrics, maxQueueSize, SocketServer.DataPlaneMetricPrefix)
    queueSizeSensor.add(queueSizePercentiles)

    // simulate that queue size is consistently over queue size threshold
    for (i <- 0 until 100000) {
      queueSizeSensor.record(config.backpressureConfig.queueSizeCap + 1)
    }
    assertBackpressureMetricValue("non-exempt-request-time-capacity", Some(expectedBrokerQuotaLimit), 0.01)
    requestQuotaManager.updateBrokerQuotaLimit(time.milliseconds())
    assertBackpressureMetricValue("non-exempt-request-time-capacity",
      Some(expectedBrokerQuotaLimit - BrokerBackpressureConfig.DefaultRequestQuotaAdjustment), 0.01)

    // once request queues are not overloaded, request limit is lifted up
    time.sleep(100000)
    requestQuotaManager.updateBrokerQuotaLimit(time.milliseconds())
    assertBackpressureMetricValue("non-exempt-request-time-capacity", Some(expectedBrokerQuotaLimit), 0.01)
  }

  @Test
  def testBrokerRequestLimitDoesNotFallBelowMinimum(): Unit = {
    val request = buildRequest()
    simulateTimeOnRequestHandlerThread(request, 500)
    val throttleMs = requestQuotaManager.maybeRecordAndGetThrottleTimeMs(request, time.milliseconds)
    assertEquals(0, throttleMs)
    time.sleep(10) // 10 millisecond
    request.recordNetworkThreadTimeCallback.foreach(record => record(10000000))

    val expectedBrokerQuotaLimit = totalCapacity * BrokerBackpressureConfig.DefaultMaxResourceUtilization
    requestQuotaManager.updateBrokerQuotaLimit(time.milliseconds())
    assertBackpressureMetricValue("non-exempt-request-time-capacity", Some(expectedBrokerQuotaLimit), 0.01)

    // simulate request overload
    val queueSizeSensor = metrics.sensor("RequestQueueSize")
    val queueSizePercentiles = RequestQueueSizePercentiles.createPercentiles(metrics, maxQueueSize, SocketServer.DataPlaneMetricPrefix)
    queueSizeSensor.add(queueSizePercentiles)
    for (i <- 0 until 100000) {
      // this tests that the backpressure implementation uses p90 (non-default, set in config) vs. p95 (default) to decide
      // when to backpressure; this will result in p95 = 390 < 400 (queue size threshold) and p99 = 406 > 400
      queueSizeSensor.record(i % 410)
    }
    // make sure that limit does not fall below the minimum
    for (i <- 0 until 1000) {
      requestQuotaManager.updateBrokerQuotaLimit(time.milliseconds())
    }
    assertBackpressureMetricValue("non-exempt-request-time-capacity",
      Some(config.backpressureConfig.minBrokerRequestQuota), 0.01)
  }

  @Test
  def testMultipleTenantEndpoints(): Unit = {
    recreateRequestQuotaManagerWithTwoTenantEnpoints()

    // two requests, one per listener
    val request1 = buildRequest()
    simulateTimeOnRequestHandlerThread(request1, 2)
    val throttleMs1 = requestQuotaManager.maybeRecordAndGetThrottleTimeMs(request1, time.milliseconds)
    assertEquals(0, throttleMs1)

    val request2 = buildRequest(secondListener)
    simulateTimeOnRequestHandlerThread(request2, 2)
    val throttleMs2 = requestQuotaManager.maybeRecordAndGetThrottleTimeMs(request2, time.milliseconds)
    assertEquals(0, throttleMs2)

    assertIoThreadUsageMetricValue("request-io-time", Some(0.4), 0.01)
    assertIoThreadUsageMetricValue("request-non-exempt-io-time", Some(0.4), 0.01)
    assertNetworkThreadUsageMetricValue("request-network-time", testListener.value, None, 0.01)
    assertNetworkThreadUsageMetricValue("request-network-time", secondListener.value, None, 0.01)
    assertNetworkThreadUsageMetricValue("request-non-exempt-network-time", testListener.value, None, 0.01)
    assertNetworkThreadUsageMetricValue("request-non-exempt-network-time", secondListener.value, None, 0.01)

    requestQuotaManager.updateBrokerQuotaLimit(time.milliseconds())
    // there are two network threadpools in this test
    val threadsCapacity = ioThreadpoolCapacity + networkThreadpoolCapacity + networkThreadpoolCapacity
    val expectedBrokerQuotaLimit = threadsCapacity * BrokerBackpressureConfig.DefaultMaxResourceUtilization
    assertBackpressureMetricValue("non-exempt-request-time-capacity", Some(expectedBrokerQuotaLimit), 0.01)

    time.sleep(1) // 1 millisecond
    request1.recordNetworkThreadTimeCallback.foreach(record => record(1000000))
    request2.recordNetworkThreadTimeCallback.foreach(record => record(1000000))

    assertIoThreadUsageMetricValue("request-io-time", Some(0.4), 0.01)
    assertIoThreadUsageMetricValue("request-non-exempt-io-time", Some(0.4), 0.01)
    assertNetworkThreadUsageMetricValue("request-network-time", testListener.value, Some(0.1), 0.01)
    assertNetworkThreadUsageMetricValue("request-network-time", secondListener.value, Some(0.1), 0.01)
    // verify combined usage on all multitenant network threads
    assertEquals("request-network-time",
                 0.2,
                 ThreadUsageMetrics.networkThreadsUsage(metrics, Seq(testListener.value, secondListener.value)),
                 0.01)
    assertNetworkThreadUsageMetricValue("request-non-exempt-network-time", testListener.value, Some(0.1), 0.01)
    assertNetworkThreadUsageMetricValue("request-non-exempt-network-time", secondListener.value, Some(0.1), 0.01)
    assertEquals("request-non-exempt-network-time",
                 0.2,
                 ThreadUsageMetrics.networkThreadsUsage(metrics, Seq(testListener.value, secondListener.value), Some(NonExemptRequest)),
                 0.01)

    // broker quota limit should not change since extra non-exempt network usage is tiny
    requestQuotaManager.updateBrokerQuotaLimit(time.milliseconds())
    assertBackpressureMetricValue("non-exempt-request-time-capacity", Some(expectedBrokerQuotaLimit), 0.01)
  }

  @Test
  def testRemoveListenerMetrics(): Unit = {
    val request = buildRequest()

    // Generate and verify listener metrics
    simulateTimeOnRequestHandlerThread(request, 10)
    requestQuotaManager.maybeRecordAndGetThrottleTimeMs(request, time.milliseconds)
    time.sleep(1000)
    for (_ <- 0 until networkThreadpoolSize) {
      request.recordNetworkThreadTimeCallback.foreach(record => record(1000000000))
    }
    val expectedNetworkThreadpoolUsage = networkThreadpoolSize * 100.0
    assertNetworkThreadUsageMetricValue(s"request-network-time", Some(expectedNetworkThreadpoolUsage), 1)
    assertNetworkThreadUsageMetricValue(s"request-non-exempt-network-time", Some(expectedNetworkThreadpoolUsage), 1)

    // Remove listener metrics and verify
    requestQuotaManager.removeListenerMetrics(testListener.value)
    assertNetworkThreadUsageMetricValue(s"request-network-time", None, 1)
    assertNetworkThreadUsageMetricValue(s"request-non-exempt-network-time", None, 1)

  }

  private def simulateTimeOnRequestHandlerThread(request: RequestChannel.Request, ms: Long): Unit = {
    request.requestDequeueTimeNanos = time.nanoseconds()
    time.sleep(ms) // this is time on request handler thread
    request.apiLocalCompleteTimeNanos = time.nanoseconds()
  }

  private def assertBackpressureMetricValue(metricName: String, expectedValueOpt: Option[Double], delta: Double): Unit = {
    assertMetricValue(metricName, "backpressure-metrics", Map.empty, expectedValueOpt, delta)
  }

  private def assertIoThreadUsageMetricValue(metricName: String, expectedValueOpt: Option[Double], delta: Double): Unit = {
    assertMetricValue(metricName,
                      ThreadUsageMetrics.MetricGroup,
                      ThreadUsageMetrics.ioThreadUsageMetricTags,
                      expectedValueOpt,
                      delta)
  }

  private def assertNetworkThreadUsageMetricValue(metricName: String, expectedValueOpt: Option[Double], delta: Double): Unit = {
    assertNetworkThreadUsageMetricValue(metricName, testListener.value, expectedValueOpt, delta)
  }

  private def assertNetworkThreadUsageMetricValue(metricName: String,
                                                  listener: String,
                                                  expectedValueOpt: Option[Double],
                                                  delta: Double): Unit = {
    assertMetricValue(metricName,
                      ThreadUsageMetrics.MetricGroup,
                      ThreadUsageMetrics.listenerNetworkThreadUsageMetricTags(listener),
                      expectedValueOpt,
                      delta)
  }

  private def assertMetricValue(metricName: String,
                                group: String,
                                metricTags: Map[String, String],
                                expectedValueOpt: Option[Double],
                                delta: Double): Unit = {
    val metric = metrics.metric(metrics.metricName(metricName, group, "", metricTags.asJava))
    expectedValueOpt match {
      case Some(expectedValue) =>
        assertEquals(metricName, expectedValue, metric.metricValue.asInstanceOf[Double], delta)
      case _ => assertNull(metric)
    }
  }

  private def buildRequest(): RequestChannel.Request = {
    buildRequest(testListener)
  }

  private def buildRequest(listenerName: ListenerName): RequestChannel.Request = {
    val builder = new OffsetFetchRequest.Builder("test-group", true,
      List(testTopicPartition).asJava, false)
    val request = builder.build()
    val buffer = request.serialize(new RequestHeader(builder.apiKey, request.version, testClient, 0))
    val requestChannelMetrics: RequestChannel.Metrics = EasyMock.createNiceMock(classOf[RequestChannel.Metrics])

    // read the header from the buffer first so that the body can be read next from the Request constructor
    val header = RequestHeader.parse(buffer)
    val context = new RequestContext(header, "1", InetAddress.getLocalHost, testPrincipal, listenerName, SecurityProtocol.PLAINTEXT, ClientInformation.EMPTY)
    new RequestChannel.Request(processor = 1, context, startTimeNanos =  0, MemoryPool.NONE, buffer, requestChannelMetrics)
  }

  private def maybeRecord(user: String, clientId: String, value: Double): Int = {
    val principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, user)
    val session = Session(principal, null)
    requestQuotaManager.maybeRecordAndGetThrottleTimeMs(session, clientId, value, time.milliseconds())
  }

  def millisToPercent(millis: Double): Double = millis * 1000 * 1000 * ClientQuotaManagerConfig.NanosToPercentagePerSecond

}
