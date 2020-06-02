/*
 * Copyright 2020 Confluent Inc.
 */

package unit.kafka.server.link

import java.util
import java.util.{Optional, UUID}
import java.util.concurrent.TimeUnit

import kafka.controller.KafkaController
import kafka.server.link.ClusterLinkTopicState.Mirror
import kafka.server.link._
import kafka.server.{DelegationTokenManager, KafkaConfig}
import kafka.tier.topic.TierTopicManager
import kafka.zk.{BrokerInfo, KafkaZkClient}
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

import scala.collection.mutable
import scala.jdk.CollectionConverters._

class ClusterLinkSyncOffsetsTest {

  private val scheduler = new ClusterLinkScheduler()
  private val admin: ConfluentAdmin = mock(classOf[ConfluentAdmin])
  private val destAdmin: Admin = mock(classOf[Admin])

  private val clientManager: ClusterLinkClientManager = mock(classOf[ClusterLinkClientManager])
  private val testTopicState: ClusterLinkTopicState = new Mirror("testLink", UUID.randomUUID())

  private var controller: KafkaController = null

  private val allowAllFilter =
    """
      |{
      |"groupFilters": [
      |  {
      |     "name": "*",
      |     "patternType": "LITERAL",
      |     "filterType": "WHITELIST"
      |  }]
      |}
      """.stripMargin

  @Before
  def setUp(): Unit = {
    scheduler.startup()
  }

  @After
  def tearDown(): Unit = {
    scheduler.shutdown()
  }

  @Test
  def testMigrateOffsets(): Unit = {

    setupMock(true)

    val topic = "testTopic"
    val groupName = "testGroup"

    val clusterLinkConfig: ClusterLinkConfig = new ClusterLinkConfig(Map(
      ClusterLinkConfig.ConsumerOffsetSyncEnableProp -> "true",
      ClusterLinkConfig.ConsumerOffsetGroupFiltersProp -> allowAllFilter,
    ).asJava)

    val consumerGroups = List[ConsumerGroupListing] (consumerGroupListing(groupName))
    val consumerGroupsFuture = new KafkaFutureImpl[util.Collection[ConsumerGroupListing]]
    consumerGroupsFuture.complete(consumerGroups.asJava);
    val listConsumerGroupsResult: ListConsumerGroupsResult = createMock(classOf[ListConsumerGroupsResult])
    expect(listConsumerGroupsResult.all).andReturn(consumerGroupsFuture).anyTimes()
    expect(admin.listConsumerGroups()).andReturn(listConsumerGroupsResult).times(1)

    val offsetEntries = Map[TopicPartition,OffsetAndMetadata](
      new TopicPartition(topic,1) -> offsetAndMetadata(1)
    )
    val consumerGroupOffsetsFuture = new KafkaFutureImpl[util.Map[TopicPartition,OffsetAndMetadata]]
    consumerGroupOffsetsFuture.complete(offsetEntries.asJava);
    val listConsumerGroupOffsetsResult: ListConsumerGroupOffsetsResult = createMock(classOf[ListConsumerGroupOffsetsResult])
    expect(listConsumerGroupOffsetsResult.partitionsToOffsetAndMetadata).andReturn(consumerGroupOffsetsFuture)

    val alterConsumerGroupOffsetsFuture = new KafkaFutureImpl[Void]
    alterConsumerGroupOffsetsFuture.complete(null)
    val alterConsumerGroupOffsetsResult: AlterConsumerGroupOffsetsResult = createMock(classOf[AlterConsumerGroupOffsetsResult])
    expect(alterConsumerGroupOffsetsResult.all).andReturn(alterConsumerGroupOffsetsFuture)

    expect(admin.listConsumerGroupOffsets(groupName)).andReturn(listConsumerGroupOffsetsResult)
    expect(destAdmin.alterConsumerGroupOffsets(groupName,offsetEntries.asJava)).andReturn(alterConsumerGroupOffsetsResult)

    replay(admin,clientManager,alterConsumerGroupOffsetsResult,listConsumerGroupsResult,listConsumerGroupOffsetsResult,destAdmin)

    val syncOffsets = new ClusterLinkSyncOffsets(clientManager,clusterLinkConfig,controller,() => destAdmin)
    syncOffsets.runOnce().get(5, TimeUnit.SECONDS)

    verifyMock()
  }

  @Test
  def testDoesNotRunIfNotController(): Unit = {
    // set this node to not be the controller
    setupMock(false)

    val clusterLinkConfig: ClusterLinkConfig = new ClusterLinkConfig(Map(
      ClusterLinkConfig.ConsumerOffsetSyncEnableProp -> "true",
      ClusterLinkConfig.ConsumerOffsetGroupFiltersProp -> allowAllFilter,
    ).asJava)

    replay(admin,clientManager,destAdmin)

    // this should return without performing any actions
    val syncOffsets = new ClusterLinkSyncOffsets(clientManager,clusterLinkConfig,controller,() => destAdmin)
    syncOffsets.runOnce().get(5, TimeUnit.SECONDS)

    verifyMock()
  }

  @Test
  def testMigrateOffsetsWithNoGroupFilter(): Unit = {
    setupMock(true)

    val clusterLinkConfig: ClusterLinkConfig = new ClusterLinkConfig(Map(
      ClusterLinkConfig.ConsumerOffsetSyncEnableProp -> "true",
    ).asJava)

    replay(admin,clientManager,destAdmin)

    val syncOffsets = new ClusterLinkSyncOffsets(clientManager,clusterLinkConfig,controller,() => destAdmin)
    syncOffsets.runOnce().get(5, TimeUnit.SECONDS)

    verifyMock()
  }

  @Test
  def testCurrentOffsetsAreUpdated(): Unit = {
    setupMock(true)

    val topic = "testTopic"
    val groupName = "testGroup"

    val clusterLinkConfig: ClusterLinkConfig = new ClusterLinkConfig(Map(
      ClusterLinkConfig.ConsumerOffsetSyncEnableProp -> "true",
      ClusterLinkConfig.ConsumerOffsetGroupFiltersProp -> allowAllFilter,
    ).asJava)

    val consumerGroups = List[ConsumerGroupListing] (consumerGroupListing(groupName))
    val consumerGroupsFuture = new KafkaFutureImpl[util.Collection[ConsumerGroupListing]]
    consumerGroupsFuture.complete(consumerGroups.asJava);
    val listConsumerGroupsResult: ListConsumerGroupsResult = createMock(classOf[ListConsumerGroupsResult])
    expect(listConsumerGroupsResult.all).andReturn(consumerGroupsFuture).anyTimes()
    expect(admin.listConsumerGroups()).andReturn(listConsumerGroupsResult).times(1)

    val offsetEntries = Map[TopicPartition,OffsetAndMetadata](
      new TopicPartition(topic,1) -> offsetAndMetadata(1)
    )
    val consumerGroupOffsetsFuture = new KafkaFutureImpl[util.Map[TopicPartition,OffsetAndMetadata]]
    consumerGroupOffsetsFuture.complete(offsetEntries.asJava);
    val listConsumerGroupOffsetsResult: ListConsumerGroupOffsetsResult = createMock(classOf[ListConsumerGroupOffsetsResult])
    expect(listConsumerGroupOffsetsResult.partitionsToOffsetAndMetadata).andReturn(consumerGroupOffsetsFuture)

    val alterConsumerGroupOffsetsFuture = new KafkaFutureImpl[Void]
    alterConsumerGroupOffsetsFuture.complete(null)
    val alterConsumerGroupOffsetsResult: AlterConsumerGroupOffsetsResult = createMock(classOf[AlterConsumerGroupOffsetsResult])
    expect(alterConsumerGroupOffsetsResult.all).andReturn(alterConsumerGroupOffsetsFuture)

    expect(admin.listConsumerGroupOffsets(groupName)).andReturn(listConsumerGroupOffsetsResult)
    expect(destAdmin.alterConsumerGroupOffsets(groupName,offsetEntries.asJava)).andReturn(alterConsumerGroupOffsetsResult)

    replay(admin,clientManager,alterConsumerGroupOffsetsResult,listConsumerGroupsResult,listConsumerGroupOffsetsResult,destAdmin)

    val syncOffsets = new ClusterLinkSyncOffsets(clientManager,clusterLinkConfig,controller,() => destAdmin)
    syncOffsets.runOnce().get(5, TimeUnit.SECONDS)

    assertEquals(1,syncOffsets.currentOffsets.size)
    assertEquals(1,syncOffsets.currentOffsets.get((groupName,new TopicPartition(topic,1))).get)

    verifyMock()
  }

  @Test
  def testFiltersGroupListingWithLiteral(): Unit = {
    setupMock(true)

    val topic = "testTopic"
    val validGroupName = "validGroup"
    val invalidGroupName = "invalidGroup"

    val allowOneFilter =
    s"""
      |{
      |"groupFilters": [
      |  {
      |     "name": "$validGroupName",
      |     "patternType": "LITERAL",
      |     "filterType": "WHITELIST"
      |  }]
      |}
      """.stripMargin


    val clusterLinkConfig: ClusterLinkConfig = new ClusterLinkConfig(Map(
      ClusterLinkConfig.ConsumerOffsetSyncEnableProp -> "true",
      ClusterLinkConfig.ConsumerOffsetGroupFiltersProp -> allowOneFilter,
    ).asJava)

    val consumerGroups = List[ConsumerGroupListing] (consumerGroupListing(validGroupName),consumerGroupListing(invalidGroupName))
    val consumerGroupsFuture = new KafkaFutureImpl[util.Collection[ConsumerGroupListing]]
    consumerGroupsFuture.complete(consumerGroups.asJava);
    val listConsumerGroupsResult: ListConsumerGroupsResult = createMock(classOf[ListConsumerGroupsResult])
    expect(listConsumerGroupsResult.all).andReturn(consumerGroupsFuture).anyTimes()
    expect(admin.listConsumerGroups()).andReturn(listConsumerGroupsResult).times(1)

    val offsetEntries = Map[TopicPartition,OffsetAndMetadata](
      new TopicPartition(topic,1) -> offsetAndMetadata(1)
    )
    val consumerGroupOffsetsFuture = new KafkaFutureImpl[util.Map[TopicPartition,OffsetAndMetadata]]
    consumerGroupOffsetsFuture.complete(offsetEntries.asJava);
    val listConsumerGroupOffsetsResult: ListConsumerGroupOffsetsResult = createMock(classOf[ListConsumerGroupOffsetsResult])
    expect(listConsumerGroupOffsetsResult.partitionsToOffsetAndMetadata).andReturn(consumerGroupOffsetsFuture)

    val alterConsumerGroupOffsetsFuture = new KafkaFutureImpl[Void]
    alterConsumerGroupOffsetsFuture.complete(null)
    val alterConsumerGroupOffsetsResult: AlterConsumerGroupOffsetsResult = createMock(classOf[AlterConsumerGroupOffsetsResult])
    expect(alterConsumerGroupOffsetsResult.all).andReturn(alterConsumerGroupOffsetsFuture)

    expect(admin.listConsumerGroupOffsets(validGroupName)).andReturn(listConsumerGroupOffsetsResult)
    expect(destAdmin.alterConsumerGroupOffsets(validGroupName,offsetEntries.asJava)).andReturn(alterConsumerGroupOffsetsResult)

    replay(admin,clientManager,alterConsumerGroupOffsetsResult,listConsumerGroupsResult,listConsumerGroupOffsetsResult,destAdmin)

    val syncOffsets = new ClusterLinkSyncOffsets(clientManager,clusterLinkConfig,controller,() => destAdmin)
    syncOffsets.runOnce().get(5, TimeUnit.SECONDS)

    verifyMock()
  }

  @Test
  def testFiltersMultipleGroupsWithPrefix(): Unit = {
    setupMock(true)

    val topic = "testTopic"
    val validGroupName = "validGroup"
    val validGroupName1 = "validGroup1"
    val invalidGroupName = "invalidGroup"

    val allowPrefixFilter =
      s"""
        |{
        |"groupFilters": [
        |  {
        |     "name": "$validGroupName",
        |     "patternType": "PREFIXED",
        |     "filterType": "WHITELIST"
        |  }]
        |}
      """.stripMargin

    val clusterLinkConfig: ClusterLinkConfig = new ClusterLinkConfig(Map(
      ClusterLinkConfig.ConsumerOffsetSyncEnableProp -> "true",
      ClusterLinkConfig.ConsumerOffsetGroupFiltersProp -> allowPrefixFilter,
    ).asJava)

    val consumerGroups = List[ConsumerGroupListing] (consumerGroupListing(validGroupName),consumerGroupListing(validGroupName1),consumerGroupListing(invalidGroupName))
    val consumerGroupsFuture = new KafkaFutureImpl[util.Collection[ConsumerGroupListing]]
    consumerGroupsFuture.complete(consumerGroups.asJava);
    val listConsumerGroupsResult: ListConsumerGroupsResult = createMock(classOf[ListConsumerGroupsResult])
    expect(listConsumerGroupsResult.all).andReturn(consumerGroupsFuture).anyTimes()
    expect(admin.listConsumerGroups()).andReturn(listConsumerGroupsResult).times(1)

    val offsetEntries = Map[TopicPartition,OffsetAndMetadata](
      new TopicPartition(topic,1) -> offsetAndMetadata(1)
    )
    val consumerGroupOffsetsFuture = new KafkaFutureImpl[util.Map[TopicPartition,OffsetAndMetadata]]
    consumerGroupOffsetsFuture.complete(offsetEntries.asJava);
    val listConsumerGroupOffsetsResult: ListConsumerGroupOffsetsResult = createMock(classOf[ListConsumerGroupOffsetsResult])
    expect(listConsumerGroupOffsetsResult.partitionsToOffsetAndMetadata).andReturn(consumerGroupOffsetsFuture)

    val alterConsumerGroupOffsetsFuture = new KafkaFutureImpl[Void]
    alterConsumerGroupOffsetsFuture.complete(null)
    val alterConsumerGroupOffsetsResult: AlterConsumerGroupOffsetsResult = createMock(classOf[AlterConsumerGroupOffsetsResult])
    expect(alterConsumerGroupOffsetsResult.all).andReturn(alterConsumerGroupOffsetsFuture)

    expect(admin.listConsumerGroupOffsets(validGroupName)).andReturn(listConsumerGroupOffsetsResult).times(1)
    expect(admin.listConsumerGroupOffsets(validGroupName1)).andReturn(listConsumerGroupOffsetsResult).times(1)
    expect(destAdmin.alterConsumerGroupOffsets(validGroupName,offsetEntries.asJava)).andReturn(alterConsumerGroupOffsetsResult)

    replay(admin,clientManager,alterConsumerGroupOffsetsResult,listConsumerGroupsResult,listConsumerGroupOffsetsResult,destAdmin)

    val syncOffsets = new ClusterLinkSyncOffsets(clientManager,clusterLinkConfig,controller,() => destAdmin)
    syncOffsets.runOnce().get(5, TimeUnit.SECONDS)

    verifyMock()
  }

  @Test
  def testFiltersBlacklistedGroup(): Unit = {
    setupMock(true)

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

    val clusterLinkConfig: ClusterLinkConfig = new ClusterLinkConfig(Map(
      ClusterLinkConfig.ConsumerOffsetSyncEnableProp -> "true",
      ClusterLinkConfig.ConsumerOffsetGroupFiltersProp -> blackListFilter,
    ).asJava)

    val consumerGroups = List[ConsumerGroupListing] (consumerGroupListing(validGroupName),consumerGroupListing(validGroupName1),consumerGroupListing(invalidGroupName))
    val consumerGroupsFuture = new KafkaFutureImpl[util.Collection[ConsumerGroupListing]]
    consumerGroupsFuture.complete(consumerGroups.asJava);
    val listConsumerGroupsResult: ListConsumerGroupsResult = createMock(classOf[ListConsumerGroupsResult])
    expect(listConsumerGroupsResult.all).andReturn(consumerGroupsFuture).anyTimes()
    expect(admin.listConsumerGroups()).andReturn(listConsumerGroupsResult).times(1)

    val offsetEntries = Map[TopicPartition,OffsetAndMetadata](
      new TopicPartition(topic,1) -> offsetAndMetadata(1)
    )
    val consumerGroupOffsetsFuture = new KafkaFutureImpl[util.Map[TopicPartition,OffsetAndMetadata]]
    consumerGroupOffsetsFuture.complete(offsetEntries.asJava);
    val listConsumerGroupOffsetsResult: ListConsumerGroupOffsetsResult = createMock(classOf[ListConsumerGroupOffsetsResult])
    expect(listConsumerGroupOffsetsResult.partitionsToOffsetAndMetadata).andReturn(consumerGroupOffsetsFuture)

    val alterConsumerGroupOffsetsFuture = new KafkaFutureImpl[Void]
    alterConsumerGroupOffsetsFuture.complete(null)
    val alterConsumerGroupOffsetsResult: AlterConsumerGroupOffsetsResult = createMock(classOf[AlterConsumerGroupOffsetsResult])
    expect(alterConsumerGroupOffsetsResult.all).andReturn(alterConsumerGroupOffsetsFuture)

    expect(admin.listConsumerGroupOffsets(validGroupName)).andReturn(listConsumerGroupOffsetsResult)
    expect(admin.listConsumerGroupOffsets(validGroupName1)).andReturn(listConsumerGroupOffsetsResult)
    expect(destAdmin.alterConsumerGroupOffsets(validGroupName,offsetEntries.asJava)).andReturn(alterConsumerGroupOffsetsResult)

    replay(admin,clientManager,alterConsumerGroupOffsetsResult,listConsumerGroupsResult,listConsumerGroupOffsetsResult,destAdmin)

    val syncOffsets = new ClusterLinkSyncOffsets(clientManager,clusterLinkConfig,controller,() => destAdmin)
    syncOffsets.runOnce().get(5, TimeUnit.SECONDS)

    verifyMock()
  }

  @Test
  def testFiltersMultipleBlacklistedGroupWithPrefix(): Unit = {
    setupMock(true)

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

    val clusterLinkConfig: ClusterLinkConfig = new ClusterLinkConfig(Map(
      ClusterLinkConfig.ConsumerOffsetSyncEnableProp -> "true",
      ClusterLinkConfig.ConsumerOffsetGroupFiltersProp -> blackListFilter,
    ).asJava)

    val consumerGroups = List[ConsumerGroupListing] (consumerGroupListing(validGroupName),consumerGroupListing(validGroupName1),consumerGroupListing(invalidGroupName),consumerGroupListing(invalidGroupName2))
    val consumerGroupsFuture = new KafkaFutureImpl[util.Collection[ConsumerGroupListing]]
    consumerGroupsFuture.complete(consumerGroups.asJava);
    val listConsumerGroupsResult: ListConsumerGroupsResult = createMock(classOf[ListConsumerGroupsResult])
    expect(listConsumerGroupsResult.all).andReturn(consumerGroupsFuture).anyTimes()
    expect(admin.listConsumerGroups()).andReturn(listConsumerGroupsResult).times(1)

    val offsetEntries = Map[TopicPartition,OffsetAndMetadata](
      new TopicPartition(topic,1) -> offsetAndMetadata(1)
    )
    val consumerGroupOffsetsFuture = new KafkaFutureImpl[util.Map[TopicPartition,OffsetAndMetadata]]
    consumerGroupOffsetsFuture.complete(offsetEntries.asJava);
    val listConsumerGroupOffsetsResult: ListConsumerGroupOffsetsResult = createMock(classOf[ListConsumerGroupOffsetsResult])
    expect(listConsumerGroupOffsetsResult.partitionsToOffsetAndMetadata).andReturn(consumerGroupOffsetsFuture)

    val alterConsumerGroupOffsetsFuture = new KafkaFutureImpl[Void]
    alterConsumerGroupOffsetsFuture.complete(null)
    val alterConsumerGroupOffsetsResult: AlterConsumerGroupOffsetsResult = createMock(classOf[AlterConsumerGroupOffsetsResult])
    expect(alterConsumerGroupOffsetsResult.all).andReturn(alterConsumerGroupOffsetsFuture)

    expect(admin.listConsumerGroupOffsets(validGroupName)).andReturn(listConsumerGroupOffsetsResult)
    expect(admin.listConsumerGroupOffsets(validGroupName1)).andReturn(listConsumerGroupOffsetsResult)
    expect(destAdmin.alterConsumerGroupOffsets(validGroupName,offsetEntries.asJava)).andReturn(alterConsumerGroupOffsetsResult)

    replay(admin,clientManager,alterConsumerGroupOffsetsResult,listConsumerGroupsResult,listConsumerGroupOffsetsResult,destAdmin)

    val syncOffsets = new ClusterLinkSyncOffsets(clientManager,clusterLinkConfig,controller,() => destAdmin)
    syncOffsets.runOnce().get(5, TimeUnit.SECONDS)

    verifyMock()
  }

  @Test
  def testDoesNotUpdateUnchangedOffsets(): Unit = {
    setupMock(true)

    val topic = "testTopic"
    val groupName = "testGroup"

    val clusterLinkConfig: ClusterLinkConfig = new ClusterLinkConfig(Map(
      ClusterLinkConfig.ConsumerOffsetSyncEnableProp -> "true",
      ClusterLinkConfig.ConsumerOffsetGroupFiltersProp -> allowAllFilter,
    ).asJava)

    val consumerGroups = List[ConsumerGroupListing] (consumerGroupListing(groupName))
    val consumerGroupsFuture = new KafkaFutureImpl[util.Collection[ConsumerGroupListing]]
    consumerGroupsFuture.complete(consumerGroups.asJava);
    val listConsumerGroupsResult: ListConsumerGroupsResult = createMock(classOf[ListConsumerGroupsResult])
    expect(listConsumerGroupsResult.all).andReturn(consumerGroupsFuture).anyTimes()
    expect(admin.listConsumerGroups()).andReturn(listConsumerGroupsResult).times(1)

    val offsetEntries = Map[TopicPartition,OffsetAndMetadata](
      new TopicPartition(topic,1) -> offsetAndMetadata(1)
    )
    val consumerGroupOffsetsFuture = new KafkaFutureImpl[util.Map[TopicPartition,OffsetAndMetadata]]
    consumerGroupOffsetsFuture.complete(offsetEntries.asJava);
    val listConsumerGroupOffsetsResult: ListConsumerGroupOffsetsResult = createMock(classOf[ListConsumerGroupOffsetsResult])
    expect(listConsumerGroupOffsetsResult.partitionsToOffsetAndMetadata).andReturn(consumerGroupOffsetsFuture)

    expect(admin.listConsumerGroupOffsets(groupName)).andReturn(listConsumerGroupOffsetsResult)

    replay(admin,clientManager,listConsumerGroupsResult,listConsumerGroupOffsetsResult,destAdmin)

    val syncOffsets = new ClusterLinkSyncOffsets(clientManager,clusterLinkConfig,controller,() => destAdmin)

    // we force the existing offset to be the same as the fetched one, this should not call alterConsumerGroupOffsets
    syncOffsets.currentOffsets.clear()
    syncOffsets.currentOffsets += (groupName,new TopicPartition(topic,1)) -> 1
    syncOffsets.runOnce().get(5, TimeUnit.SECONDS)

    verifyMock()
  }

  @Test
  def testOldGroupsAreRemovedFromCurrentOffsets(): Unit = {
    setupMock(true)

    val topic = "testTopic"
    val groupName = "testGroup"
    val oldGroupName = "oldGroup"

    val clusterLinkConfig: ClusterLinkConfig = new ClusterLinkConfig(Map(
      ClusterLinkConfig.ConsumerOffsetSyncEnableProp -> "true",
      ClusterLinkConfig.ConsumerOffsetGroupFiltersProp -> allowAllFilter,
    ).asJava)

    val consumerGroups = List[ConsumerGroupListing] (consumerGroupListing(groupName))
    val consumerGroupsFuture = new KafkaFutureImpl[util.Collection[ConsumerGroupListing]]
    consumerGroupsFuture.complete(consumerGroups.asJava);
    val listConsumerGroupsResult: ListConsumerGroupsResult = createMock(classOf[ListConsumerGroupsResult])
    expect(listConsumerGroupsResult.all).andReturn(consumerGroupsFuture).anyTimes()
    expect(admin.listConsumerGroups()).andReturn(listConsumerGroupsResult).times(1)

    val offsetEntries = Map[TopicPartition,OffsetAndMetadata](
      new TopicPartition(topic,1) -> offsetAndMetadata(1)
    )
    val consumerGroupOffsetsFuture = new KafkaFutureImpl[util.Map[TopicPartition,OffsetAndMetadata]]
    consumerGroupOffsetsFuture.complete(offsetEntries.asJava);
    val listConsumerGroupOffsetsResult: ListConsumerGroupOffsetsResult = createMock(classOf[ListConsumerGroupOffsetsResult])
    expect(listConsumerGroupOffsetsResult.partitionsToOffsetAndMetadata).andReturn(consumerGroupOffsetsFuture)

    val alterConsumerGroupOffsetsFuture = new KafkaFutureImpl[Void]
    alterConsumerGroupOffsetsFuture.complete(null)
    val alterConsumerGroupOffsetsResult: AlterConsumerGroupOffsetsResult = createMock(classOf[AlterConsumerGroupOffsetsResult])
    expect(alterConsumerGroupOffsetsResult.all).andReturn(alterConsumerGroupOffsetsFuture)


    expect(admin.listConsumerGroupOffsets(groupName)).andReturn(listConsumerGroupOffsetsResult)
    expect(destAdmin.alterConsumerGroupOffsets(groupName,offsetEntries.asJava)).andReturn(alterConsumerGroupOffsetsResult)

    replay(admin,clientManager,alterConsumerGroupOffsetsResult,listConsumerGroupsResult,listConsumerGroupOffsetsResult,destAdmin)

    val syncOffsets = new ClusterLinkSyncOffsets(clientManager,clusterLinkConfig,controller,() => destAdmin)

    syncOffsets.currentOffsets.clear()
    syncOffsets.currentOffsets += (oldGroupName,new TopicPartition(topic,1)) -> 1
    syncOffsets.runOnce().get(5, TimeUnit.SECONDS)

    assertFalse(syncOffsets.currentOffsets.contains(oldGroupName,new TopicPartition(topic,1)))

    verifyMock()
  }

  @Test
  def testDoesNotCommitNonMirroredTopics(): Unit = {
    setupMock(true)

    val topic = "testTopic"
    val nonMirroredTopic = "nonMirroredTopic"
    val groupName = "testGroup"

    val clusterLinkConfig: ClusterLinkConfig = new ClusterLinkConfig(Map(
      ClusterLinkConfig.ConsumerOffsetSyncEnableProp -> "true",
      ClusterLinkConfig.ConsumerOffsetGroupFiltersProp -> allowAllFilter,
    ).asJava)

    val consumerGroups = List[ConsumerGroupListing] (consumerGroupListing(groupName))
    val consumerGroupsFuture = new KafkaFutureImpl[util.Collection[ConsumerGroupListing]]
    consumerGroupsFuture.complete(consumerGroups.asJava);
    val listConsumerGroupsResult: ListConsumerGroupsResult = createMock(classOf[ListConsumerGroupsResult])
    expect(listConsumerGroupsResult.all).andReturn(consumerGroupsFuture).anyTimes()
    expect(admin.listConsumerGroups()).andReturn(listConsumerGroupsResult).times(1)

    val validOffsetEntries = mutable.Map[TopicPartition,OffsetAndMetadata](
      new TopicPartition(topic,1) -> offsetAndMetadata(1)
    )
    val invalidOffsetEntries = mutable.Map[TopicPartition,OffsetAndMetadata](
      new TopicPartition(nonMirroredTopic,1) -> offsetAndMetadata(1)
    )

    val consumerGroupOffsetsFuture = new KafkaFutureImpl[util.Map[TopicPartition,OffsetAndMetadata]]
    consumerGroupOffsetsFuture.complete((validOffsetEntries ++ invalidOffsetEntries).asJava);
    val listConsumerGroupOffsetsResult: ListConsumerGroupOffsetsResult = createMock(classOf[ListConsumerGroupOffsetsResult])
    expect(listConsumerGroupOffsetsResult.partitionsToOffsetAndMetadata).andReturn(consumerGroupOffsetsFuture)

    val alterConsumerGroupOffsetsFuture = new KafkaFutureImpl[Void]
    alterConsumerGroupOffsetsFuture.complete(null)
    val alterConsumerGroupOffsetsResult: AlterConsumerGroupOffsetsResult = createMock(classOf[AlterConsumerGroupOffsetsResult])
    expect(alterConsumerGroupOffsetsResult.all).andReturn(alterConsumerGroupOffsetsFuture)


    expect(admin.listConsumerGroupOffsets(groupName)).andReturn(listConsumerGroupOffsetsResult)
    expect(destAdmin.alterConsumerGroupOffsets(groupName,validOffsetEntries.asJava)).andReturn(alterConsumerGroupOffsetsResult)

    replay(admin,clientManager,alterConsumerGroupOffsetsResult,listConsumerGroupsResult,listConsumerGroupOffsetsResult,destAdmin)

    val syncOffsets = new ClusterLinkSyncOffsets(clientManager,clusterLinkConfig,controller,() => destAdmin)
    syncOffsets.runOnce().get(5, TimeUnit.SECONDS)

    verifyMock()
  }


  @Test
  def testNoGroupAuthOnOffsetCommit(): Unit = {
    setupMock(true)

    val topic = "testTopic"
    val groupName = "testGroup"

    val clusterLinkConfig: ClusterLinkConfig = new ClusterLinkConfig(Map(
      ClusterLinkConfig.ConsumerOffsetSyncEnableProp -> "true",
      ClusterLinkConfig.ConsumerOffsetGroupFiltersProp -> allowAllFilter,
    ).asJava)

    val consumerGroups = List[ConsumerGroupListing] (consumerGroupListing(groupName))
    val consumerGroupsFuture = new KafkaFutureImpl[util.Collection[ConsumerGroupListing]]
    consumerGroupsFuture.complete(consumerGroups.asJava);
    val listConsumerGroupsResult: ListConsumerGroupsResult = createMock(classOf[ListConsumerGroupsResult])
    expect(listConsumerGroupsResult.all).andReturn(consumerGroupsFuture).anyTimes()
    expect(admin.listConsumerGroups()).andReturn(listConsumerGroupsResult).times(1)

    val offsetEntries = Map[TopicPartition,OffsetAndMetadata](
      new TopicPartition(topic,1) -> offsetAndMetadata(1)
    )
    val consumerGroupOffsetsFuture = new KafkaFutureImpl[util.Map[TopicPartition,OffsetAndMetadata]]
    consumerGroupOffsetsFuture.complete(offsetEntries.asJava);
    val listConsumerGroupOffsetsResult: ListConsumerGroupOffsetsResult = createMock(classOf[ListConsumerGroupOffsetsResult])
    expect(listConsumerGroupOffsetsResult.partitionsToOffsetAndMetadata).andReturn(consumerGroupOffsetsFuture)

    val alterConsumerGroupOffsetsFuture = new KafkaFutureImpl[Void]
    alterConsumerGroupOffsetsFuture.completeExceptionally(new GroupAuthorizationException("no group auth"))
    val alterConsumerGroupOffsetsResult: AlterConsumerGroupOffsetsResult = createMock(classOf[AlterConsumerGroupOffsetsResult])
    expect(alterConsumerGroupOffsetsResult.all).andReturn(alterConsumerGroupOffsetsFuture)


    expect(admin.listConsumerGroupOffsets(groupName)).andReturn(listConsumerGroupOffsetsResult)
    expect(destAdmin.alterConsumerGroupOffsets(groupName,offsetEntries.asJava)).andReturn(alterConsumerGroupOffsetsResult)

    replay(admin,clientManager,alterConsumerGroupOffsetsResult,listConsumerGroupsResult,listConsumerGroupOffsetsResult,destAdmin)

    // this exception should be caught with a warn logged so nothing to do here except verify no exception is thrown.
    val syncOffsets = new ClusterLinkSyncOffsets(clientManager,clusterLinkConfig,controller,() => destAdmin)
    syncOffsets.runOnce().get(5, TimeUnit.SECONDS)

    verifyMock()
  }

  @Test
  def testNoTopicAuthOnOffsetCommit(): Unit = {
    setupMock(true)

    val topic = "testTopic"
    val groupName = "testGroup"

    val clusterLinkConfig: ClusterLinkConfig = new ClusterLinkConfig(Map(
      ClusterLinkConfig.ConsumerOffsetSyncEnableProp -> "true",
      ClusterLinkConfig.ConsumerOffsetGroupFiltersProp -> allowAllFilter,
    ).asJava)

    val consumerGroups = List[ConsumerGroupListing] (consumerGroupListing(groupName))
    val consumerGroupsFuture = new KafkaFutureImpl[util.Collection[ConsumerGroupListing]]
    consumerGroupsFuture.complete(consumerGroups.asJava);
    val listConsumerGroupsResult: ListConsumerGroupsResult = createMock(classOf[ListConsumerGroupsResult])
    expect(listConsumerGroupsResult.all).andReturn(consumerGroupsFuture).anyTimes()
    expect(admin.listConsumerGroups()).andReturn(listConsumerGroupsResult).times(1)

    val offsetEntries = Map[TopicPartition,OffsetAndMetadata](
      new TopicPartition(topic,1) -> offsetAndMetadata(1)
    )
    val consumerGroupOffsetsFuture = new KafkaFutureImpl[util.Map[TopicPartition,OffsetAndMetadata]]
    consumerGroupOffsetsFuture.complete(offsetEntries.asJava);
    val listConsumerGroupOffsetsResult: ListConsumerGroupOffsetsResult = createMock(classOf[ListConsumerGroupOffsetsResult])
    expect(listConsumerGroupOffsetsResult.partitionsToOffsetAndMetadata).andReturn(consumerGroupOffsetsFuture)

    val alterConsumerGroupOffsetsFuture = new KafkaFutureImpl[Void]
    alterConsumerGroupOffsetsFuture.completeExceptionally(new TopicAuthorizationException("no topic auth"))
    val alterConsumerGroupOffsetsResult: AlterConsumerGroupOffsetsResult = createMock(classOf[AlterConsumerGroupOffsetsResult])
    expect(alterConsumerGroupOffsetsResult.all).andReturn(alterConsumerGroupOffsetsFuture)


    expect(admin.listConsumerGroupOffsets(groupName)).andReturn(listConsumerGroupOffsetsResult)
    expect(destAdmin.alterConsumerGroupOffsets(groupName,offsetEntries.asJava)).andReturn(alterConsumerGroupOffsetsResult)

    replay(admin,clientManager,alterConsumerGroupOffsetsResult,listConsumerGroupsResult,listConsumerGroupOffsetsResult,destAdmin)

    // this exception should be caught with a warn logged so nothing to do here except verify no exception is thrown.
    val syncOffsets = new ClusterLinkSyncOffsets(clientManager,clusterLinkConfig,controller,() => destAdmin)
    syncOffsets.runOnce().get(5, TimeUnit.SECONDS)

    verifyMock()
  }

  def offsetAndMetadata(offset: Long): OffsetAndMetadata = {
    new OffsetAndMetadata(offset,Optional.empty(),"")
  }

  def consumerGroupListing(name:String): ConsumerGroupListing = {
    new ConsumerGroupListing(name,true)
  }


  private def setupMock(isController:Boolean, linkedTopics: Map[String, ClusterLinkTopicState] = Map[String, ClusterLinkTopicState] ("testTopic" -> testTopicState)): Unit = {
    reset(admin)
    reset(clientManager)

    expect(clientManager.scheduler).andReturn(scheduler).anyTimes()
    expect(clientManager.getAdmin).andReturn(admin).anyTimes()
    controller=new KafkaControllerTest(testLinkedTopics = linkedTopics,testIsActive=isController)
  }

  private def verifyMock(): Unit = {
    verify(clientManager)
    verify(admin)
  }
}


class KafkaControllerTest(override val config: KafkaConfig = new KafkaConfig(Map[String,String]("zookeeper.connect" -> "somehost:2181").asJava),
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
