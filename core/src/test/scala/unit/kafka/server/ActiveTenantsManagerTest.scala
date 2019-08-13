/*
 Copyright 2019 Confluent Inc.
 */

package kafka.server

import java.lang.management.ManagementFactory

import javax.management.ObjectName
import org.apache.kafka.common.metrics.{JmxReporter, Metrics}
import org.apache.kafka.common.utils.MockTime
import org.junit.Assert.{assertEquals, assertTrue}
import org.junit.{After, Before, Test}

class ActiveTenantsManagerTest {
  private val time = new MockTime

  private var metrics: Metrics = _
  private var activeTenantsManager: ActiveTenantsManager = _

  @Before
  def beforeMethod(): Unit = {
    metrics = new Metrics()
    activeTenantsManager = new ActiveTenantsManager(metrics, time, 2)
  }

  @After
  def afterMethod(): Unit = {
    metrics.close()
  }

  @Test
  def testGetActiveTenantsPrunesInactiveTenants(): Unit = {
    val activeTenants = scala.collection.mutable.Set[Map[String, String]]()
    val metricTags1 = metricTags("", "Client1")
    val metricTags2 = metricTags("", "Client2")

    activeTenantsManager.trackActiveTenant(metricTags1, time.milliseconds())
    activeTenants += metricTags1
    assertEquals(activeTenants, activeTenantsManager.getActiveTenants())

    time.sleep(1)
    activeTenantsManager.trackActiveTenant(metricTags2, time.milliseconds())
    activeTenants += metricTags2
    assertEquals(activeTenants, activeTenantsManager.getActiveTenants())

    time.sleep(1)
    activeTenantsManager.trackActiveTenant(metricTags1, time.milliseconds())
    activeTenants += metricTags1
    assertEquals(activeTenants, activeTenantsManager.getActiveTenants())

    time.sleep(2)
    // Client2 should not be an active tenant anymore
    activeTenants -= metricTags2
    assertEquals(activeTenants, activeTenantsManager.getActiveTenants())

    time.sleep(3)
    // All clients should be removed once $timeWindowMs passes
    activeTenants -= metricTags1
    activeTenants -= metricTags2
    assertEquals(activeTenants, activeTenantsManager.getActiveTenants())
  }

  @Test
  def testRecordActiveTenantsMetric(): Unit = {
    val server = ManagementFactory.getPlatformMBeanServer
    val mBeanName = "kafka.server:type=multi-tenant-metrics"
    val reporter = new JmxReporter("kafka.server")
    metrics.addReporter(reporter)

    val metricTags1 = metricTags("", "Client1")
    val metricTags2 = metricTags("", "Client2")

    def numActiveTenants: Double = {
      server.getAttribute(new ObjectName(mBeanName), "active-tenants-count").asInstanceOf[Double]
    }

    // Check initialization
    assertTrue(server.isRegistered(new ObjectName(mBeanName)))
    assertEquals(0, numActiveTenants, 0)
    assertTrue(reporter.containsMbean(mBeanName))

    time.sleep(1)
    activeTenantsManager.trackActiveTenant(metricTags1, time.milliseconds())
    // Metric only records every $timeWindowMs milliseconds, so value should not change
    assertEquals(0, numActiveTenants, 0)

    time.sleep(3)
    activeTenantsManager.trackActiveTenant(metricTags2, time.milliseconds())
    // Client1 becomes inactive and Client2 becomes active
    assertEquals(1, numActiveTenants, 0)
  }

  def metricTags(user: String, clientId: String): Map[String, String] = {
    Map("user" -> user, "client-id" -> clientId)
  }
}
