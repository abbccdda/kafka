/*
 * Copyright 2020 Confluent Inc.
 */

package kafka.server.link

import java.util.{Collections, Properties}
import java.util.concurrent.{ExecutionException, TimeUnit}

import kafka.server.ConfigType
import kafka.zk.{AdminZkClient, KafkaZkClient}
import org.apache.kafka.clients.admin.{ConfluentAdmin, Config, ConfigEntry, DescribeConfigsResult}
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.internals.KafkaFutureImpl
import org.apache.kafka.common.errors.TopicAuthorizationException
import org.easymock.{EasyMock => EM}
import org.easymock.EasyMock._
import org.junit.{After, Before, Test}
import org.junit.Assert._

import org.scalatest.Assertions.intercept

import scala.jdk.CollectionConverters._

class ClusterLinkSyncTopicsConfigsTest {

  private val scheduler = new ClusterLinkScheduler()
  private val admin: ConfluentAdmin = mock(classOf[ConfluentAdmin])
  private val zkClient: KafkaZkClient = mock(classOf[KafkaZkClient])
  private val adminZkClient: AdminZkClient = mock(classOf[AdminZkClient])
  private val clientManager: ClusterLinkClientManager = mock(classOf[ClusterLinkClientManager])

  @Before
  def setUp(): Unit = {
    scheduler.startup()
  }

  @After
  def tearDown(): Unit = {
    scheduler.shutdown()
  }

  @Test
  def testUpdateConfigs(): Unit = {
    setupMock()

    val topic = "test-topic"
    expect(clientManager.getTopics).andReturn(Set(topic)).times(1)
    replay(clientManager)

    val config = new KafkaFutureImpl[Config]
    config.complete(new Config(List(
      new ConfigEntry("cleanup.policy", "compact"),
      new ConfigEntry("segment.ms", "100000")
    ).asJavaCollection))

    val resource = newConfigResource(topic)
    val describeConfigsArg = java.util.Collections.singleton(resource)
    val describeConfigsResult = new DescribeConfigsResult(Collections.unmodifiableMap(Map(resource -> config).asJava))
    expect(admin.describeConfigs(EM.eq(describeConfigsArg))).andReturn(describeConfigsResult).times(1)
    replay(admin)

    val curProps = new Properties()
    curProps.put("cleanup.policy", "delete")
    curProps.put("unclean.leader.election.enable", "true")
    expect(adminZkClient.fetchEntityConfig(EM.eq(ConfigType.Topic), EM.eq(topic))).andReturn(curProps).times(1)

    val newProps = new Properties()
    newProps.put("cleanup.policy", "compact")
    newProps.put("segment.ms", "100000")
    newProps.put("unclean.leader.election.enable", "true")
    expect(adminZkClient.changeTopicConfig(EM.eq(topic), EM.eq(newProps))).times(1)
    replay(adminZkClient)

    val syncTopicsConfigs = new ClusterLinkSyncTopicsConfigs(clientManager, syncIntervalMs = 100)
    syncTopicsConfigs.runOnce().get(5, TimeUnit.SECONDS)

    verifyMock()
  }

  @Test
  def testNoUpdateConfigs(): Unit = {
    setupMock()

    val topic = "test-topic"
    expect(clientManager.getTopics).andReturn(Set(topic)).times(1)
    replay(clientManager)

    val config = new KafkaFutureImpl[Config]
    config.complete(new Config(List(new ConfigEntry("cleanup.policy", "compact")).asJavaCollection))

    val resource = newConfigResource(topic)
    val describeConfigsArg = java.util.Collections.singleton(resource)
    val describeConfigsResult = new DescribeConfigsResult(Collections.unmodifiableMap(Map(resource -> config).asJava))
    expect(admin.describeConfigs(EM.eq(describeConfigsArg))).andReturn(describeConfigsResult).times(1)
    replay(admin)

    val curProps = new Properties()
    curProps.put("cleanup.policy", "compact")
    expect(adminZkClient.fetchEntityConfig(EM.eq(ConfigType.Topic), EM.eq(topic))).andReturn(curProps).times(1)
    replay(adminZkClient)

    val syncTopicsConfigs = new ClusterLinkSyncTopicsConfigs(clientManager, syncIntervalMs = 100)
    syncTopicsConfigs.runOnce().get(5, TimeUnit.SECONDS)

    verifyMock()
  }

  @Test
  def testNoRepeatedLocalFetchConfigsOnNoChange(): Unit = {
    setupMock()

    val topic = "test-topic"
    expect(clientManager.getTopics).andReturn(Set(topic)).times(2)
    replay(clientManager)

    val config = new KafkaFutureImpl[Config]
    config.complete(new Config(List(new ConfigEntry("cleanup.policy", "compact")).asJavaCollection))

    val resource = newConfigResource(topic)
    val describeConfigsArg = java.util.Collections.singleton(resource)
    val describeConfigsResult = new DescribeConfigsResult(Collections.unmodifiableMap(Map(resource -> config).asJava))
    expect(admin.describeConfigs(EM.eq(describeConfigsArg))).andReturn(describeConfigsResult).times(2)
    replay(admin)

    val curProps = new Properties()
    curProps.put("cleanup.policy", "compact")
    expect(adminZkClient.fetchEntityConfig(EM.eq(ConfigType.Topic), EM.eq(topic))).andReturn(curProps).times(1)
    replay(adminZkClient)

    val syncTopicsConfigs = new ClusterLinkSyncTopicsConfigs(clientManager, syncIntervalMs = 100)
    syncTopicsConfigs.runOnce().get(5, TimeUnit.SECONDS)
    syncTopicsConfigs.runOnce().get(5, TimeUnit.SECONDS)

    verifyMock()
  }

  @Test
  def testExceptionFetchingConfigs(): Unit = {
    setupMock()

    val topic = "test-topic"
    expect(clientManager.getTopics).andReturn(Set(topic)).times(1)
    replay(clientManager)

    val resource = newConfigResource(topic)
    val describeConfigsArg = java.util.Collections.singleton(resource)
    expect(admin.describeConfigs(EM.eq(describeConfigsArg))).andThrow(new TopicAuthorizationException("")).times(1)
    replay(admin)

    replay(adminZkClient)

    val syncTopicsConfigs = new ClusterLinkSyncTopicsConfigs(clientManager, syncIntervalMs = 100)
    val e = intercept[ExecutionException] {
      syncTopicsConfigs.runOnce().get(5, TimeUnit.SECONDS)
    }
    assertTrue(e.getCause.isInstanceOf[TopicAuthorizationException])

    verifyMock()
  }

  @Test
  def testExceptionConfigsResult(): Unit = {
    setupMock()

    val topics = List("test-topic-1", "test-topic-2", "test-topic-3")
    expect(clientManager.getTopics).andReturn(topics.toSet).times(1)
    replay(clientManager)

    val successConfig = new KafkaFutureImpl[Config]
    successConfig.complete(new Config(List(new ConfigEntry("cleanup.policy", "compact")).asJavaCollection))
    val errorConfig = new KafkaFutureImpl[Config]
    errorConfig.completeExceptionally(new TopicAuthorizationException(""))

    val resources = topics.map(newConfigResource)
    val describeConfigsArg = new java.util.HashSet[ConfigResource](3)
    resources.foreach(describeConfigsArg.add)

    val describeConfigsResult = new DescribeConfigsResult(
      Collections.unmodifiableMap(Map(resources(0) -> successConfig, resources(1) -> errorConfig, resources(2) -> successConfig).asJava))
    expect(admin.describeConfigs(EM.eq(describeConfigsArg))).andReturn(describeConfigsResult).times(1)
    replay(admin)

    val curProps = new Properties()
    curProps.put("cleanup.policy", "delete")
    expect(adminZkClient.fetchEntityConfig(EM.eq(ConfigType.Topic), EM.eq(topics(0)))).andReturn(curProps).times(1)
    expect(adminZkClient.fetchEntityConfig(EM.eq(ConfigType.Topic), EM.eq(topics(2)))).andReturn(curProps).times(1)

    val newProps = new Properties()
    newProps.put("cleanup.policy", "compact")
    expect(adminZkClient.changeTopicConfig(EM.eq(topics(0)), EM.eq(newProps))).times(1)
    expect(adminZkClient.changeTopicConfig(EM.eq(topics(2)), EM.eq(newProps))).times(1)

    replay(adminZkClient)

    val syncTopicsConfigs = new ClusterLinkSyncTopicsConfigs(clientManager, syncIntervalMs = 100)
    syncTopicsConfigs.runOnce().get(5, TimeUnit.SECONDS)

    verifyMock()
  }

  @Test
  def testChangeTopics(): Unit = {
    setupMock()

    val topics = List("test-topic-1", "test-topic-2")
    expect(clientManager.getTopics).andReturn(Set(topics(0))).times(1)
    expect(clientManager.getTopics).andReturn(Set(topics(1))).times(1)
    replay(clientManager)

    val config1 = new KafkaFutureImpl[Config]
    config1.complete(new Config(List(new ConfigEntry("cleanup.policy", "compact")).asJavaCollection))
    val config2 = new KafkaFutureImpl[Config]
    config2.complete(new Config(List(new ConfigEntry("retention.ms", "1000000")).asJavaCollection))

    val resources = topics.map(newConfigResource)
    val describeConfigsArg1 = java.util.Collections.singleton(resources(0))
    val describeConfigsArg2 = java.util.Collections.singleton(resources(1))
    val describeConfigsResult1 =
      new DescribeConfigsResult(Collections.unmodifiableMap(Map(resources(0) -> config1).asJava))
    val describeConfigsResult2 =
      new DescribeConfigsResult(Collections.unmodifiableMap(Map(resources(1) -> config2).asJava))
    expect(admin.describeConfigs(EM.eq(describeConfigsArg1))).andReturn(describeConfigsResult1).times(1)
    expect(admin.describeConfigs(EM.eq(describeConfigsArg2))).andReturn(describeConfigsResult2).times(1)
    replay(admin)

    val curProps1 = new Properties()
    curProps1.put("cleanup.policy", "compact")
    val curProps2 = new Properties()
    curProps2.put("retention.ms", "1000000")
    expect(adminZkClient.fetchEntityConfig(EM.eq(ConfigType.Topic), EM.eq(topics(0)))).andReturn(curProps1).times(1)
    expect(adminZkClient.fetchEntityConfig(EM.eq(ConfigType.Topic), EM.eq(topics(1)))).andReturn(curProps2).times(1)
    replay(adminZkClient)

    val syncTopicsConfigs = new ClusterLinkSyncTopicsConfigs(clientManager, syncIntervalMs = 100)
    syncTopicsConfigs.runOnce().get(5, TimeUnit.SECONDS)
    syncTopicsConfigs.runOnce().get(5, TimeUnit.SECONDS)

    verifyMock()
  }

  private def newConfigResource(topic: String): ConfigResource =
    new ConfigResource(ConfigResource.Type.TOPIC, topic)

  private def setupMock(): Unit = {
    reset(admin)
    reset(adminZkClient)
    reset(clientManager)

    expect(clientManager.scheduler).andReturn(scheduler).anyTimes()
    expect(clientManager.getAdmin).andReturn(admin).anyTimes()
    expect(clientManager.zkClient).andReturn(zkClient).anyTimes()
    expect(clientManager.adminZkClient).andReturn(adminZkClient).anyTimes()
  }

  private def verifyMock(): Unit = {
    verify(clientManager)
    verify(admin)
    verify(adminZkClient)
  }
}
