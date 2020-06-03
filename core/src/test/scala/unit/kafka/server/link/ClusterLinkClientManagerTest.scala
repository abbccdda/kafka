/*
 * Copyright 2020 Confluent Inc.
 */

package kafka.server.link

import java.util.{Properties, UUID}

import kafka.controller.KafkaController
import kafka.utils.Implicits._
import kafka.zk.{ClusterLinkData, KafkaZkClient}
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.admin.{Admin, ConfluentAdmin}
import org.apache.kafka.server.authorizer.Authorizer
import org.easymock.EasyMock._
import org.junit.Assert._
import org.junit.Test
import org.scalatest.Assertions.intercept

class ClusterLinkClientManagerTest {

  val scheduler: ClusterLinkScheduler = createNiceMock(classOf[ClusterLinkScheduler])
  val zkClient: KafkaZkClient = createNiceMock(classOf[KafkaZkClient])
  var clientManager: ClusterLinkClientManager = _
  val authorizer: Authorizer = createNiceMock(classOf[Authorizer])
  val controller: KafkaController = createNiceMock(classOf[KafkaController])
  val destAdmin: Admin = createNiceMock(classOf[Admin])

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

    val clientManager = newClientManager(linkName, factoryConfig, adminFactory, Some(authorizer), controller)
    assertEquals(0, factoryCalled)

    clientManager.startup()
    try {
      assertEquals(1, factoryCalled)
      assertTrue(factoryAdmin eq clientManager.getAdmin)

      factoryAdmin = createNiceMock(classOf[ConfluentAdmin])
      factoryConfig = newConfig(Map(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG -> "localhost:2345"))
      clientManager.reconfigure(factoryConfig, Set(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG))
      assertEquals(2, factoryCalled)
      assertTrue(factoryAdmin eq clientManager.getAdmin)

      factoryConfig = newConfig(Map(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG -> "localhost:2345",
        ClusterLinkConfig.NumClusterLinkFetchersProp -> "5"))
      clientManager.reconfigure(factoryConfig, Set(ClusterLinkConfig.NumClusterLinkFetchersProp))
      assertEquals(2, factoryCalled)
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
    val clientManager = newClientManager(linkName, config, adminFactory, Some(authorizer), controller)
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
  @Test
  def testAclSyncTaskStartup(): Unit = {
    val linkName = "test-link"
    val migrateAllAclsJson =
      """{
        | "aclFilters": [{
        |  "resourceFilter": {
        |      "resourceType": "any",
        |      "patternType": "any"
        |    },
        |  "accessFilter": {
        |     "operation": "any",
        |     "permissionType": "any"
        |    }
        |  }]
        | }""".stripMargin
    val config = newConfig(Map(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG -> "localhost:1234",
      ClusterLinkConfig.AclSyncEnableProp -> "true",
      ClusterLinkConfig.AclFiltersProp -> migrateAllAclsJson))
    def adminFactory(config: ClusterLinkConfig): ConfluentAdmin =
      createNiceMock(classOf[ConfluentAdmin])

    val clientManager = newClientManager(linkName, config, adminFactory, Some(authorizer),
      controller)
    clientManager.startup()

    try {
      assert(clientManager.getSyncAclTask.isDefined)
    } finally {
      clientManager.shutdown()
    }
  }

  @Test
  def testAclSyncTaskStartupWithNoAuthorizer(): Unit = {
    val linkName = "test-link"
    val migrateAllAclsJson =
      """{
        | "aclFilters": [{
        |  "resourceFilter": {
        |      "resourceType": "any",
        |      "patternType": "any"
        |    },
        |  "accessFilter": {
        |     "operation": "any",
        |     "permissionType": "any"
        |    }
        |  }]
        | }""".stripMargin
    val config = newConfig(Map(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG -> "localhost:1234",
      ClusterLinkConfig.AclSyncEnableProp -> "true",
      ClusterLinkConfig.AclFiltersProp -> migrateAllAclsJson))
    def adminFactory(config: ClusterLinkConfig): ConfluentAdmin =
      createNiceMock(classOf[ConfluentAdmin])

    val clientManager = newClientManager(linkName, config, adminFactory, None, controller)

    try {
      clientManager.startup()
    } catch {
      case e: IllegalArgumentException =>
        assertEquals(e.getMessage, "ACL migration is enabled but authorizer.class.name is" +
          " not set. Please set authorizer.class.name to proceed with ACL migration.")
    }
    finally {
      clientManager.shutdown()
    }
  }

  @Test
  def testAclSyncTaskNoStartup(): Unit = {
    val linkName = "test-link"
    val config = newConfig(Map(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG -> "localhost:1234",
      ClusterLinkConfig.AclSyncEnableProp -> "false"))
    def adminFactory(config: ClusterLinkConfig): ConfluentAdmin =
      createNiceMock(classOf[ConfluentAdmin])

    val clientManager = newClientManager(linkName, config, adminFactory, Some(authorizer),
      controller)
    clientManager.startup()

    try {
      assert(clientManager.getSyncAclTask.isEmpty)
    }
    finally {
      clientManager.shutdown()
    }
  }

  private def newClientManager(linkName: String,
                               config: ClusterLinkConfig,
                               adminFactory: ClusterLinkConfig => ConfluentAdmin,
                               authorizer: Option[Authorizer],
                               controller: KafkaController) = {
    expect(scheduler.schedule(anyString(), anyObject(), anyLong(), anyLong(), anyObject())).andReturn(null).anyTimes()
    replay(scheduler)
    val linkData = ClusterLinkData(linkName, UUID.randomUUID, None, None)
    new ClusterLinkClientManager(linkData, scheduler, zkClient, config,  authorizer, controller, adminFactory,() => destAdmin)
  }

  private def newConfig(configs: Map[String, String]): ClusterLinkConfig = {
    val props = new Properties()
    props ++= configs
    new ClusterLinkConfig(props)
  }
}
