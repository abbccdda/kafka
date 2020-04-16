/*
 * Copyright 2020 Confluent Inc.
 */

package kafka.server.link

import java.util.Properties

import kafka.utils.Implicits._
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.admin.ConfluentAdmin
import org.easymock.EasyMock.createNiceMock
import org.junit.Test
import org.junit.Assert._
import org.scalatest.Assertions.intercept

import scala.jdk.CollectionConverters._

class ClusterLinkClientManagerTest {

  var clientManager: ClusterLinkClientManager = _

  @Test
  def testAdmin(): Unit = {
    val linkName = "test-link"

    var factoryCalled = 0
    var factoryConfig = newConfig(Map(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG -> "localhost:1234"))
    var factoryAdmin: ConfluentAdmin = createNiceMock(classOf[ConfluentAdmin])

    def adminFactory(config: ClusterLinkConfig): ConfluentAdmin = {
      factoryCalled += 1
      assertTrue(factoryConfig eq config)
      factoryAdmin
    }

    val clientManager = new ClusterLinkClientManager(linkName, factoryConfig, adminFactory)
    assertEquals(0, factoryCalled)

    clientManager.startup()
    try {
      assertEquals(1, factoryCalled)
      assertTrue(factoryAdmin eq clientManager.getAdmin)

      factoryAdmin = createNiceMock(classOf[ConfluentAdmin])
      factoryConfig = newConfig(Map(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG -> "localhost:2345"))
      clientManager.reconfigure(factoryConfig)
      assertEquals(2, factoryCalled)
      assertTrue(factoryAdmin eq clientManager.getAdmin)
    } finally {
      clientManager.shutdown()
    }

    assertEquals(2, factoryCalled)

    intercept[IllegalStateException] {
      clientManager.getAdmin
    }
  }

  @Test
  def testTopics(): Unit = {
    val linkName = "test-link"
    val config = newConfig(Map(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG -> "localhost:1234"))
    def adminFactory(config: ClusterLinkConfig): ConfluentAdmin = createNiceMock(classOf[ConfluentAdmin])
    val clientManager = new ClusterLinkClientManager(linkName, config, adminFactory)
    val topics = List("topic0", "topic1", "topic2")

    clientManager.startup()
    try {
      assertEquals(Set.empty, clientManager.getTopics)

      clientManager.addTopics(Set(topics(0), topics(1)))
      assertEquals(Set(topics(0), topics(1)), clientManager.getTopics)

      clientManager.addTopics(Set(topics(1), topics(2)))
      assertEquals(Set(topics(0), topics(1), topics(2)), clientManager.getTopics)

      clientManager.removeTopics(Set(topics(2), topics(0)))
      assertEquals(Set(topics(1)), clientManager.getTopics)

      clientManager.removeTopics(Set(topics(1), "unknown"))
      assertEquals(Set.empty, clientManager.getTopics)
    } finally {
      clientManager.shutdown()
    }
  }

  private def newConfig(configs: Map[String, String]): ClusterLinkConfig = {
    val props = new Properties()
    props ++= configs
    new ClusterLinkConfig(props)
  }

}
