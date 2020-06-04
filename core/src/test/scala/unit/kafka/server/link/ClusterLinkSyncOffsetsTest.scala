/*
 * Copyright 2020 Confluent Inc.
 */

package kafka.server.link

import java.util
import java.util.{Collections, Optional, UUID}
import java.util.concurrent.TimeUnit

import kafka.controller.KafkaController
import kafka.server.link.ClusterLinkTopicState.Mirror
import kafka.server.{DelegationTokenManager, KafkaConfig}
import kafka.tier.topic.TierTopicManager
import kafka.zk.{BrokerInfo, ClusterLinkData, KafkaZkClient}
import org.apache.kafka.clients.admin._
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.{GroupAuthorizationException, TopicAuthorizationException}
import org.apache.kafka.common.internals.KafkaFutureImpl
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.utils.Time
import org.easymock.EasyMock._
import org.junit.{After, Before, Test}
import org.junit.Assert._

import scala.jdk.CollectionConverters._

class ClusterLinkSyncOffsetsTest {

  private val scheduler = new ClusterLinkScheduler()
  private val admin: ConfluentAdmin = mock(classOf[ConfluentAdmin])
  private val destAdmin: Admin = mock(classOf[Admin])

  private val clientManager: ClusterLinkClientManager = mock(classOf[ClusterLinkClientManager])
  private val testTopicState: ClusterLinkTopicState = new Mirror("testLink", UUID.randomUUID())
  private val metrics: Metrics = new Metrics()

  private var controller: KafkaController = null

  private val allowAllFilter = offsetFilter(GroupFilter("*", "LITERAL", "WHITELIST"))

  @Before
  def setUp(): Unit = {
    scheduler.startup()
  }

  @After
  def tearDown(): Unit = {
    scheduler.shutdown()
    metrics.close()
  }

  @Test
  def testMigrateOffsets(): Unit = {

    setupMock(isController = true)

    val topic = "testTopic"
    val groupName = "testGroup"

    setupMockListGroupsResponse(groupName)
    val offsetEntries = Map(new TopicPartition(topic, 1) -> offsetAndMetadata(1))
    setupMockOffsetResponses(offsetEntries, groupName)

    val clusterLinkConfig = linkConfig(allowAllFilter)
    syncOffsetsAndVerify(clusterLinkConfig)
  }

  @Test
  def testDoesNotRunIfNotController(): Unit = {
    // set this node to not be the controller
    setupMock(isController = false)

    val clusterLinkConfig = linkConfig(allowAllFilter)

    // this should return without performing any actions
    syncOffsetsAndVerify(clusterLinkConfig)
  }

  @Test
  def testMigrateOffsetsWithNoGroupFilter(): Unit = {
    setupMock(isController = true)

    val clusterLinkConfig: ClusterLinkConfig = new ClusterLinkConfig(Map(
      ClusterLinkConfig.ConsumerOffsetSyncEnableProp -> "true",
    ).asJava)

    syncOffsetsAndVerify(clusterLinkConfig)
  }

  @Test
  def testCurrentOffsetsAreUpdated(): Unit = {
    setupMock(isController = true)

    val topic = "testTopic"
    val groupName = "testGroup"

    val clusterLinkConfig = linkConfig(allowAllFilter)

    setupMockListGroupsResponse(groupName)
    val offsetEntries = Map(new TopicPartition(topic, 1) -> offsetAndMetadata(1))
    setupMockOffsetResponses(offsetEntries, groupName)
    val syncOffsets = syncOffsetsAndVerify(clusterLinkConfig)

    assertEquals(1, syncOffsets.currentOffsets.size)
    assertEquals(1, syncOffsets.currentOffsets((groupName, new TopicPartition(topic, 1))))
  }

  @Test
  def testFiltersGroupListingWithLiteral(): Unit = {
    setupMock(isController = true)

    val topic = "testTopic"
    val validGroupName = "validGroup"
    val invalidGroupName = "invalidGroup"

    val allowOneFilter = offsetFilter(GroupFilter(validGroupName, "LITERAL", "WHITELIST"))

    val clusterLinkConfig = linkConfig(allowOneFilter)

    setupMockListGroupsResponse(validGroupName, invalidGroupName)
    val offsetEntries = Map(new TopicPartition(topic, 1) -> offsetAndMetadata(1))
    setupMockOffsetResponses(offsetEntries, validGroupName)
    syncOffsetsAndVerify(clusterLinkConfig)
  }

  @Test
  def testFiltersMultipleGroupsWithPrefix(): Unit = {
    setupMock(isController = true)

    val topic = "testTopic"
    val validGroupName = "validGroup"
    val validGroupName1 = "validGroup1"
    val invalidGroupName = "invalidGroup"

    val allowPrefixFilter = offsetFilter(GroupFilter(validGroupName, "PREFIXED", "WHITELIST"))

    val clusterLinkConfig = linkConfig(allowPrefixFilter)

    setupMockListGroupsResponse(validGroupName, validGroupName1, invalidGroupName)

    val offsetEntries = Map(new TopicPartition(topic, 1) -> offsetAndMetadata(1))
    setupMockOffsetResponses(offsetEntries, validGroupName, validGroupName1)
    syncOffsetsAndVerify(clusterLinkConfig)
  }

  @Test
  def testFiltersBlacklistedGroup(): Unit = {
    setupMock(isController = true)

    val topic = "testTopic"
    val validGroupName = "validGroup"
    val validGroupName1 = "validGroup1"
    val invalidGroupName = "invalidGroup"

    val blackListFilter =
      s"""
         |{
         |"groupFilters": [
         |  {
         |     "name": "*",
         |     "patternType": "LITERAL",
         |     "filterType": "WHITELIST"
         |  },
         |  {
         |     "name": "$invalidGroupName",
         |     "patternType": "LITERAL",
         |     "filterType": "BLACKLIST"
         |  }
         |]}
      """.stripMargin

    val clusterLinkConfig = linkConfig(blackListFilter)

    setupMockListGroupsResponse(validGroupName, validGroupName1, invalidGroupName)
    val offsetEntries = Map(new TopicPartition(topic, 1) -> offsetAndMetadata(1))
    setupMockOffsetResponses(offsetEntries, validGroupName, validGroupName1)
    syncOffsetsAndVerify(clusterLinkConfig)
  }

  @Test
  def testFiltersMultipleBlacklistedGroupWithPrefix(): Unit = {
    setupMock(isController = true)

    val topic = "testTopic"
    val validGroupName = "validGroup"
    val validGroupName1 = "validGroup1"
    val invalidGroupName = "invalidGroup"
    val invalidGroupName2 = "invalidGroup2"

    val blackListFilter =
      s"""
         |{
         |"groupFilters": [
         |  {
         |     "name": "*",
         |     "patternType": "LITERAL",
         |     "filterType": "WHITELIST"
         |  },
         |  {
         |     "name": "$invalidGroupName",
         |     "patternType": "PREFIXED",
         |     "filterType": "BLACKLIST"
         |  }
         |]}
      """.stripMargin

    val clusterLinkConfig = linkConfig(blackListFilter)

    setupMockListGroupsResponse(validGroupName, validGroupName1, invalidGroupName, invalidGroupName2)

    val offsetEntries = Map(new TopicPartition(topic, 1) -> offsetAndMetadata(1))
    setupMockOffsetResponses(offsetEntries, validGroupName, validGroupName1)
    syncOffsetsAndVerify(clusterLinkConfig)
  }

  @Test
  def testDoesNotUpdateUnchangedOffsets(): Unit = {
    setupMock(isController = true)

    val topic = "testTopic"
    val groupName = "testGroup"

    val clusterLinkConfig = linkConfig(allowAllFilter)

    setupMockListGroupsResponse(groupName)

    val offsetEntries = Map(new TopicPartition(topic, 1) -> offsetAndMetadata(1))
    setupMockOffsetResponses(offsetEntries, groupName)

    replay(admin, clientManager, destAdmin)

    val syncOffsets = new ClusterLinkSyncOffsets(clientManager, linkData(), clusterLinkConfig,
      controller, () => destAdmin, metrics, Collections.emptyMap())

    // we force the existing offset to be the same as the fetched one, this should not call alterConsumerGroupOffsets
    syncOffsets.currentOffsets.clear()
    syncOffsets.currentOffsets += (groupName, new TopicPartition(topic, 1)) -> 1
    syncOffsets.runOnce().get(5, TimeUnit.SECONDS)

    verifyMock()
  }

  @Test
  def testOldGroupsAreRemovedFromCurrentOffsets(): Unit = {
    setupMock(isController = true)

    val topic = "testTopic"
    val groupName = "testGroup"
    val oldGroupName = "oldGroup"

    val clusterLinkConfig = linkConfig(allowAllFilter)

    setupMockListGroupsResponse(groupName)

    val offsetEntries = Map(new TopicPartition(topic, 1) -> offsetAndMetadata(1))
    setupMockOffsetResponses(offsetEntries, groupName)
    replay(admin, clientManager, destAdmin)

    val syncOffsets = new ClusterLinkSyncOffsets(clientManager, linkData(), clusterLinkConfig,
      controller,() => destAdmin, metrics, Collections.emptyMap())

    syncOffsets.currentOffsets.clear()
    syncOffsets.currentOffsets += (oldGroupName, new TopicPartition(topic, 1)) -> 1
    syncOffsets.runOnce().get(5, TimeUnit.SECONDS)

    assertFalse(syncOffsets.currentOffsets.contains(oldGroupName, new TopicPartition(topic, 1)))

    verifyMock()
  }

  @Test
  def testDoesNotCommitNonMirroredTopics(): Unit = {
    setupMock(isController = true)

    val topic = "testTopic"
    val nonMirroredTopic = "nonMirroredTopic"
    val groupName = "testGroup"

    val clusterLinkConfig = linkConfig(allowAllFilter)

    setupMockListGroupsResponse(groupName)

    val validOffsetEntries = Map(new TopicPartition(topic, 1) -> offsetAndMetadata(1))
    val invalidOffsetEntries = Map(new TopicPartition(nonMirroredTopic, 1) -> offsetAndMetadata(1))

    val listConsumerGroupOffsetsResult = mockListOffsets((validOffsetEntries ++ invalidOffsetEntries).toMap)
    val alterConsumerGroupOffsetsResult = mockAlterOffsets()
    expect(admin.listConsumerGroupOffsets(groupName)).andReturn(listConsumerGroupOffsetsResult)
    expect(destAdmin.alterConsumerGroupOffsets(groupName, validOffsetEntries.asJava)).andReturn(alterConsumerGroupOffsetsResult)

    syncOffsetsAndVerify(clusterLinkConfig)
  }


  @Test
  def testNoGroupAuthOnOffsetCommit(): Unit = {
    setupMock(isController = true)

    val topic = "testTopic"
    val groupName = "testGroup"

    val clusterLinkConfig = linkConfig(allowAllFilter)

    setupMockListGroupsResponse(groupName)

    val offsetEntries = Map(new TopicPartition(topic, 1) -> offsetAndMetadata(1))
    val listConsumerGroupOffsetsResult = mockListOffsets(offsetEntries)

    val alterConsumerGroupOffsetsFuture = new KafkaFutureImpl[Void]
    alterConsumerGroupOffsetsFuture.completeExceptionally(new GroupAuthorizationException("no group auth"))
    val alterConsumerGroupOffsetsResult: AlterConsumerGroupOffsetsResult = createMock(classOf[AlterConsumerGroupOffsetsResult])
    expect(alterConsumerGroupOffsetsResult.all).andReturn(alterConsumerGroupOffsetsFuture)
    replay(alterConsumerGroupOffsetsResult)

    expect(admin.listConsumerGroupOffsets(groupName)).andReturn(listConsumerGroupOffsetsResult)
    expect(destAdmin.alterConsumerGroupOffsets(groupName, offsetEntries.asJava)).andReturn(alterConsumerGroupOffsetsResult)


    // this exception should be caught with a warn logged so nothing to do here except verify no exception is thrown.
    syncOffsetsAndVerify(clusterLinkConfig)
  }

  @Test
  def testNoTopicAuthOnOffsetCommit(): Unit = {
    setupMock(isController = true)

    val topic = "testTopic"
    val groupName = "testGroup"

    val clusterLinkConfig = linkConfig(allowAllFilter)

    setupMockListGroupsResponse(groupName)

    val offsetEntries = Map(new TopicPartition(topic, 1) -> offsetAndMetadata(1))
    val listConsumerGroupOffsetsResult = mockListOffsets(offsetEntries)

    val alterConsumerGroupOffsetsFuture = new KafkaFutureImpl[Void]
    alterConsumerGroupOffsetsFuture.completeExceptionally(new TopicAuthorizationException("no topic auth"))
    val alterConsumerGroupOffsetsResult: AlterConsumerGroupOffsetsResult = createMock(classOf[AlterConsumerGroupOffsetsResult])
    expect(alterConsumerGroupOffsetsResult.all).andReturn(alterConsumerGroupOffsetsFuture)
    replay(alterConsumerGroupOffsetsResult)

    expect(admin.listConsumerGroupOffsets(groupName)).andReturn(listConsumerGroupOffsetsResult)
    expect(destAdmin.alterConsumerGroupOffsets(groupName, offsetEntries.asJava)).andReturn(alterConsumerGroupOffsetsResult)

    // this exception should be caught with a warn logged so nothing to do here except verify no exception is thrown.
    syncOffsetsAndVerify(clusterLinkConfig)
  }

  @Test
  def testMigrateTenantOffsetsWithLiteralFilter(): Unit = {
    verifyTenantFilter("testGroup", GroupFilter("testGroup", "LITERAL", "WHITELIST"))
  }

  @Test
  def testMigrateTenantOffsetsWithWildcardFilter(): Unit = {
    verifyTenantFilter("testGroup", GroupFilter("*", "LITERAL", "WHITELIST"))
  }

  @Test
  def testMigrateTenantOffsetsWithPrefixedFilter(): Unit = {
    verifyTenantFilter("testGroup", GroupFilter("test", "PREFIXED", "WHITELIST"))
  }

  private def verifyTenantFilter(group: String, filter: GroupFilter): Unit = {

    setupMock(isController = true)

    val topic = "testTopic"
    val prefix = "destPrefix_"
    val prefixedGroup = s"$prefix$group"

    setupMockListGroupsResponse(prefixedGroup)
    val offsetEntries = Map(new TopicPartition(topic, 2) -> offsetAndMetadata(1))
    setupMockOffsetResponses(offsetEntries, prefixedGroup)

    val clusterLinkConfig = linkConfig(offsetFilter(filter))
    syncOffsetsAndVerify(clusterLinkConfig, tenantPrefix = Some(prefix))
  }

  private def offsetFilter(groupFilter: GroupFilter): String = {
    s"""
       |{
       |"groupFilters": [
       |  {
       |     "name": "${groupFilter.name}",
       |     "patternType": "${groupFilter.patternType}",
       |     "filterType": "${groupFilter.filterType}"
       |  }]
       |}
      """.stripMargin
  }

  private def linkConfig(offsetFilter: String): ClusterLinkConfig = {
    new ClusterLinkConfig(Map(
      ClusterLinkConfig.ConsumerOffsetSyncEnableProp -> "true",
      ClusterLinkConfig.ConsumerOffsetGroupFiltersProp -> offsetFilter,
    ).asJava)
  }

  private def linkData(tenantPrefix: Option[String] = None): ClusterLinkData =
    ClusterLinkData("testLink", UUID.randomUUID, None, tenantPrefix)


  private def mockListGroups(groups: String*): ListConsumerGroupsResult = {
    val consumerGroups = groups.map(consumerGroupListing).toList
    val future = new KafkaFutureImpl[util.Collection[ConsumerGroupListing]]
    future.complete(consumerGroups.asJava)
    val result: ListConsumerGroupsResult = createMock(classOf[ListConsumerGroupsResult])
    expect(result.all).andReturn(future).anyTimes()
    replay(result)
    result
  }

  private def mockListOffsets(offsetEntries: Map[TopicPartition, OffsetAndMetadata]): ListConsumerGroupOffsetsResult = {
    val future = new KafkaFutureImpl[util.Map[TopicPartition, OffsetAndMetadata]]
    future.complete(offsetEntries.asJava)
    val result: ListConsumerGroupOffsetsResult = createMock(classOf[ListConsumerGroupOffsetsResult])
    expect(result.partitionsToOffsetAndMetadata).andReturn(future)
    replay(result)
    result
  }

  private def mockAlterOffsets(): AlterConsumerGroupOffsetsResult = {
    val future = new KafkaFutureImpl[Void]
    future.complete(null)
    val result: AlterConsumerGroupOffsetsResult = createMock(classOf[AlterConsumerGroupOffsetsResult])
    expect(result.all).andReturn(future)
    replay(result)
    result
  }

  private def syncOffsetsAndVerify(clusterLinkConfig: ClusterLinkConfig,
                                   tenantPrefix: Option[String] = None): ClusterLinkSyncOffsets = {
    replay(admin, clientManager, destAdmin)
    val syncOffsets = new ClusterLinkSyncOffsets(clientManager,
      linkData(tenantPrefix),
      clusterLinkConfig,
      controller,
      () => destAdmin,
      metrics,
      Collections.emptyMap())

    syncOffsets.runOnce().get(5, TimeUnit.SECONDS)
    verifyMock()
    syncOffsets
  }

  private def offsetAndMetadata(offset: Long): OffsetAndMetadata = {
    new OffsetAndMetadata(offset, Optional.empty(),"")
  }

  private def consumerGroupListing(name:String): ConsumerGroupListing = {
    new ConsumerGroupListing(name, true)
  }

  private def setupMockListGroupsResponse(groups: String*): Unit = {
    val listGroupsResult = mockListGroups(groups:_*)
    expect(admin.listConsumerGroups()).andReturn(listGroupsResult).times(1)
  }

  private def setupMockOffsetResponses(offsetEntries: Map[TopicPartition, OffsetAndMetadata], groups: String*): Unit = {
    val listOffsetsResult = mockListOffsets(offsetEntries)
    val alterOffsetsResult = mockAlterOffsets()

    groups.foreach { group =>
      expect(admin.listConsumerGroupOffsets(group)).andReturn(listOffsetsResult).times(1)
      expect(destAdmin.alterConsumerGroupOffsets(group, offsetEntries.asJava)).andReturn(alterOffsetsResult).times(1)
    }
  }

  private def setupMock(isController: Boolean, linkedTopics: Map[String, ClusterLinkTopicState] = Map[String, ClusterLinkTopicState] ("testTopic" -> testTopicState)): Unit = {
    reset(admin)
    reset(clientManager)

    expect(clientManager.scheduler).andReturn(scheduler).anyTimes()
    expect(clientManager.getAdmin).andReturn(admin).anyTimes()
    controller=new KafkaControllerTest(testLinkedTopics = linkedTopics, testIsActive=isController)
  }

  private def verifyMock(): Unit = {
    verify(clientManager)
    verify(admin)
  }
}


class KafkaControllerTest(override val config: KafkaConfig = new KafkaConfig(Map[String, String]("zookeeper.connect" -> "somehost:2181").asJava),
                                  zkClient: KafkaZkClient = null,
                                  time: Time = null,
                                  metrics: Metrics = null,
                                  initialBrokerInfo: BrokerInfo = null,
                                  initialBrokerEpoch: Long = 0L,
                                  tokenManager: DelegationTokenManager = null,
                                  tierTopicManagerOpt: Option[TierTopicManager] = null,
                                  threadNamePrefix: Option[String] = null,
                                  testLinkedTopics: Map[String, ClusterLinkTopicState] = Map.empty[String, ClusterLinkTopicState],
                                  testIsActive:Boolean = true)
  extends KafkaController (config,
    zkClient,
    time,
    metrics,
    initialBrokerInfo,
    initialBrokerEpoch,
    tokenManager,
    tierTopicManagerOpt,
    threadNamePrefix) {
  controllerContext.linkedTopics ++= testLinkedTopics
  override def isActive: Boolean = testIsActive
}
