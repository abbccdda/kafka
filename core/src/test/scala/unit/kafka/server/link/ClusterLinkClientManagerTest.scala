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
import org.apache.kafka.common.errors.ClusterLinkPausedException
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.server.authorizer.Authorizer
import org.easymock.EasyMock._
import org.junit.Assert._
import org.junit.{After, Test}
import org.scalatest.Assertions.intercept

class ClusterLinkClientManagerTest {

  val scheduler: ClusterLinkScheduler = createNiceMock(classOf[ClusterLinkScheduler])
  val zkClient: KafkaZkClient = createNiceMock(classOf[KafkaZkClient])
  var clientManager: ClusterLinkClientManager = _
  val authorizer: Authorizer = createNiceMock(classOf[Authorizer])
  val controller: KafkaController = createNiceMock(classOf[KafkaController])
  val destAdmin: Admin = createNiceMock(classOf[Admin])
  val metrics: Metrics = new Metrics()

  @After
  def tearDown(): Unit = {
    metrics.close()
  }

  @Test
  def testReconfigure(): Unit = {
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

      // Reconfigure with configurations affecting the admin client.
      factoryAdmin = createNiceMock(classOf[ConfluentAdmin])
      factoryConfig = newConfig(Map(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG -> "localhost:2345"))
      clientManager.reconfigure(factoryConfig, Set(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG))
      assertEquals(2, factoryCalled)
      assertTrue(factoryAdmin eq clientManager.getAdmin)

      // Reconfigure with parameters that don't affect the admin client.
      factoryConfig = newConfig(Map(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG -> "localhost:2345",
        ClusterLinkConfig.NumClusterLinkFetchersProp -> "5"))
      clientManager.reconfigure(factoryConfig, Set(ClusterLinkConfig.NumClusterLinkFetchersProp))
      assertEquals(2, factoryCalled)

      // Reconfigure by pausing the cluster link. Setting parameters that affect the admin client should
      // not have any effect.
      factoryConfig = newConfig(Map(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG -> "localhost:3456",
        ClusterLinkConfig.ClusterLinkPausedProp -> "true"))
      clientManager.reconfigure(factoryConfig, Set(ClusterLinkConfig.NumClusterLinkFetchersProp,
        CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, ClusterLinkConfig.ClusterLinkPausedProp))
      assertEquals(2, factoryCalled)
      intercept[ClusterLinkPausedException] {
        clientManager.getAdmin
      }

      // Reconfigure with the cluster link still paused.
      factoryConfig = newConfig(Map(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG -> "localhost:4567",
        ClusterLinkConfig.ClusterLinkPausedProp -> "true"))
      clientManager.reconfigure(factoryConfig, Set(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG))
      assertEquals(2, factoryCalled)
      intercept[ClusterLinkPausedException] {
        clientManager.getAdmin
      }

      // Unpause the cluster link by removing the config entry.
      factoryAdmin = createNiceMock(classOf[ConfluentAdmin])
      factoryConfig = newConfig(Map(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG -> "localhost:4567"))
      clientManager.reconfigure(factoryConfig, Set(ClusterLinkConfig.ClusterLinkPausedProp))
      assertEquals(3, factoryCalled)
      assertTrue(factoryAdmin eq clientManager.getAdmin)

    } finally {
      clientManager.shutdown()
    }

    assertEquals(3, factoryCalled)

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
    val linkData = ClusterLinkData(linkName, UUID.randomUUID, None, None, false)
    new ClusterLinkClientManager(linkData, scheduler, zkClient, config,  authorizer, controller,
      metrics, adminFactory, () => destAdmin)
  }

  private def newConfig(configs: Map[String, String]): ClusterLinkConfig = {
    val props = new Properties()
    props ++= configs
    new ClusterLinkConfig(props)
  }
}
