/*
 * Copyright 2020 Confluent Inc.
 */

package kafka.server.link

import java.util.UUID
import java.util.concurrent.{CompletableFuture, TimeUnit}

import kafka.controller.KafkaController
import kafka.zk.KafkaZkClient
import org.apache.kafka.clients.admin.{AlterMirrorsResult, ConfluentAdmin}
import org.apache.kafka.common.KafkaFuture
import org.apache.kafka.common.errors.{TimeoutException, UnknownTopicOrPartitionException}
import org.apache.kafka.common.internals.KafkaFutureImpl
import org.apache.kafka.common.requests.{AlterMirrorsRequest, AlterMirrorsResponse}
import org.easymock.{EasyMock => EM}
import org.easymock.EasyMock._
import org.junit.{After, Before, Test}
import org.junit.Assert._

import scala.jdk.CollectionConverters._

class ClusterLinkClearTopicMirrorsTest {

  private val linkName = "link-name"
  private val linkId = UUID.randomUUID()
  private val scheduler = new ClusterLinkScheduler()
  private val admin: ConfluentAdmin = mock(classOf[ConfluentAdmin])
  private val zkClient: KafkaZkClient = mock(classOf[KafkaZkClient])
  private val controller: KafkaController = mock(classOf[KafkaController])

  @Before
  def setUp(): Unit = {
    scheduler.startup()
  }

  @After
  def tearDown(): Unit = {
    scheduler.shutdown()
  }

  @Test
  def testClearTopicMirrorsEmpty(): Unit = {
    reset(controller)
    expect(controller.isActive).andReturn(true)
    replay(controller)

    reset(zkClient)
    expect(zkClient.clusterLinkExists(linkId)).andReturn(true)
    expect(zkClient.getAllTopicsInCluster(false)).andReturn(Set.empty)
    replay(zkClient)

    val done = new CompletableFuture[Void]
    val clearTopicMirrors = newClearTopicMirrors(() => done.complete(null))
    clearTopicMirrors.runOnce().get(5, TimeUnit.SECONDS)
    done.get

    verify(controller)
    verify(zkClient)
  }

  @Test
  def testClearTopicMirrorsIsMirror(): Unit = {
    val topic = "topic"

    reset(controller)
    expect(controller.isActive).andReturn(true)
    replay(controller)

    reset(zkClient)
    expect(zkClient.clusterLinkExists(linkId)).andReturn(true)
    expect(zkClient.getAllTopicsInCluster(false)).andReturn(Set(topic))
    expect(zkClient.getClusterLinkForTopics(Set(topic))).andReturn(
      Map(topic -> ClusterLinkTopicState.Mirror(linkName, linkId)))
    replay(zkClient)

    val expectedOps: List[AlterMirrorsRequest.Op] = List(new AlterMirrorsRequest.ClearTopicMirrorOp(topic))

    val result = new AlterMirrorsResponse.ClearTopicMirrorResult()
    val future = new KafkaFutureImpl[AlterMirrorsResponse.Result]
    future.complete(result)
    val ret = new AlterMirrorsResult(List(future.asInstanceOf[KafkaFuture[AlterMirrorsResponse.Result]]).asJava)

    reset(admin)
    expect(admin.alterMirrors(EM.eq(expectedOps.asJava), anyObject())).andReturn(ret)
    replay(admin)

    val done = new CompletableFuture[Void]
    val clearTopicMirrors = newClearTopicMirrors(() => done.complete(null))
    clearTopicMirrors.runOnce().get(5, TimeUnit.SECONDS)
    done.get

    verify(admin)
    verify(controller)
    verify(zkClient)
  }

  @Test
  def testClearTopicMirrorsIsMirrorDifferentLinkId(): Unit = {
    val topic = "topic"
    val otherLinkName = "other-link-name"
    val otherLinkId = UUID.randomUUID()

    reset(controller)
    expect(controller.isActive).andReturn(true)
    replay(controller)

    reset(zkClient)
    expect(zkClient.clusterLinkExists(linkId)).andReturn(true)
    expect(zkClient.getAllTopicsInCluster(false)).andReturn(Set(topic))
    expect(zkClient.getClusterLinkForTopics(Set(topic))).andReturn(
      Map(topic -> ClusterLinkTopicState.Mirror(otherLinkName, otherLinkId)))
    replay(zkClient)

    val done = new CompletableFuture[Void]
    val clearTopicMirrors = newClearTopicMirrors(() => done.complete(null))
    clearTopicMirrors.runOnce().get(5, TimeUnit.SECONDS)
    done.get

    verify(controller)
    verify(zkClient)
  }

  @Test
  def testClearTopicMirrorsIsMirrorErrorRetry(): Unit = {
    val topic = "topic"

    reset(controller)
    expect(controller.isActive).andReturn(true)
    replay(controller)

    reset(zkClient)
    expect(zkClient.clusterLinkExists(linkId)).andReturn(true)
    expect(zkClient.getAllTopicsInCluster(false)).andReturn(Set(topic))
    expect(zkClient.getClusterLinkForTopics(Set(topic))).andReturn(
      Map(topic -> ClusterLinkTopicState.Mirror(linkName, linkId))).times(2)
    replay(zkClient)

    val expectedOps: List[AlterMirrorsRequest.Op] = List(new AlterMirrorsRequest.ClearTopicMirrorOp(topic))

    val future1 = new KafkaFutureImpl[AlterMirrorsResponse.Result]
    future1.completeExceptionally(new TimeoutException())
    val ret1 = new AlterMirrorsResult(List(future1.asInstanceOf[KafkaFuture[AlterMirrorsResponse.Result]]).asJava)
    val ret2 = newAlterMirrorsResult(List(new AlterMirrorsResponse.ClearTopicMirrorResult()))

    reset(admin)
    expect(admin.alterMirrors(EM.eq(expectedOps.asJava), anyObject())).andReturn(ret1).andReturn(ret2)
    replay(admin)

    val done = new CompletableFuture[Void]
    val clearTopicMirrors = newClearTopicMirrors(() => done.complete(null))
    clearTopicMirrors.runOnce().get(5, TimeUnit.SECONDS)
    done.get

    verify(admin)
    verify(controller)
    verify(zkClient)
  }

  @Test
  def testClearTopicMirrorsIsMirrorErrorSkip(): Unit = {
    val topic = "topic"

    reset(controller)
    expect(controller.isActive).andReturn(true)
    replay(controller)

    reset(zkClient)
    expect(zkClient.clusterLinkExists(linkId)).andReturn(true)
    expect(zkClient.getAllTopicsInCluster(false)).andReturn(Set(topic))
    expect(zkClient.getClusterLinkForTopics(Set(topic))).andReturn(
      Map(topic -> ClusterLinkTopicState.Mirror(linkName, linkId)))
    replay(zkClient)

    val expectedOps: List[AlterMirrorsRequest.Op] = List(new AlterMirrorsRequest.ClearTopicMirrorOp(topic))

    val future = new KafkaFutureImpl[AlterMirrorsResponse.Result]
    future.completeExceptionally(new UnknownTopicOrPartitionException())
    val ret = new AlterMirrorsResult(List(future.asInstanceOf[KafkaFuture[AlterMirrorsResponse.Result]]).asJava)

    reset(admin)
    expect(admin.alterMirrors(EM.eq(expectedOps.asJava), anyObject())).andReturn(ret)
    replay(admin)

    val done = new CompletableFuture[Void]
    val clearTopicMirrors = newClearTopicMirrors(() => done.complete(null))
    clearTopicMirrors.runOnce().get(5, TimeUnit.SECONDS)
    done.get

    verify(admin)
    verify(controller)
    verify(zkClient)
  }

  @Test
  def testClearTopicMirrorsNotMirror(): Unit = {
    val topic = "topic"

    reset(controller)
    expect(controller.isActive).andReturn(true)
    replay(controller)

    reset(zkClient)
    expect(zkClient.clusterLinkExists(linkId)).andReturn(true)
    expect(zkClient.getAllTopicsInCluster(false)).andReturn(Set(topic))
    expect(zkClient.getClusterLinkForTopics(Set(topic))).andReturn(Map.empty)
    replay(zkClient)

    val done = new CompletableFuture[Void]
    val clearTopicMirrors = newClearTopicMirrors(() => done.complete(null))
    clearTopicMirrors.runOnce().get(5, TimeUnit.SECONDS)
    done.get

    verify(controller)
    verify(zkClient)
  }

  @Test
  def testClearTopicMirrorsLinkDoesntExist(): Unit = {
    reset(zkClient)
    expect(zkClient.clusterLinkExists(linkId)).andReturn(false).times(1)
    replay(zkClient)

    val done = new CompletableFuture[Void]
    val clearTopicMirrors = newClearTopicMirrors(() => done.complete(null))
    clearTopicMirrors.runOnce().get(5, TimeUnit.SECONDS)
    done.get

    verify(zkClient)
  }

  @Test
  def testClearTopicMirrorsNotControllerWait(): Unit = {
    reset(controller)
    expect(controller.isActive).andReturn(false).times(2)
    replay(controller)

    reset(zkClient)
    expect(zkClient.clusterLinkExists(linkId)).andReturn(true).times(2).andReturn(false)
    replay(zkClient)

    val done = new CompletableFuture[Void]
    val clearTopicMirrors = newClearTopicMirrors(() => done.complete(null))
    clearTopicMirrors.runOnce().get(5, TimeUnit.SECONDS)
    assertFalse(done.isDone)
    clearTopicMirrors.runOnce().get(5, TimeUnit.SECONDS)
    assertFalse(done.isDone)
    clearTopicMirrors.runOnce().get(5, TimeUnit.SECONDS)
    done.get

    verify(controller)
    verify(zkClient)
  }

  @Test
  def testClearTopicMirrorsControllerElected(): Unit = {
    val topic = "topic"

    reset(controller)
    expect(controller.isActive).andReturn(false).times(2).andReturn(true)
    replay(controller)

    reset(zkClient)
    expect(zkClient.clusterLinkExists(linkId)).andReturn(true).times(3)
    expect(zkClient.getAllTopicsInCluster(false)).andReturn(Set(topic))
    expect(zkClient.getClusterLinkForTopics(Set(topic))).andReturn(Map.empty)
    replay(zkClient)

    val done = new CompletableFuture[Void]
    val clearTopicMirrors = newClearTopicMirrors(() => done.complete(null))
    clearTopicMirrors.runOnce().get(5, TimeUnit.SECONDS)
    assertFalse(done.isDone)
    clearTopicMirrors.runOnce().get(5, TimeUnit.SECONDS)
    assertFalse(done.isDone)
    clearTopicMirrors.runOnce().get(5, TimeUnit.SECONDS)
    done.get

    verify(controller)
    verify(zkClient)
  }

  @Test
  def testClearTopicMirrorsControllerManyEntries(): Unit = {
    val otherLinkName = "other-link-name"
    val otherLinkId = UUID.randomUUID()

    val topicGroupSize = 2
    val topics = { for (idx <- 0 until 5) yield s"topic-$idx" }.toSet
    val topicGroups: List[Set[String]] = topics.grouped(topicGroupSize).toList

    assertEquals(3, topicGroups.size)
    assertEquals(2, topicGroups(0).size)
    assertEquals(2, topicGroups(1).size)
    assertEquals(1, topicGroups(2).size)

    reset(controller)
    expect(controller.isActive).andReturn(true)
    replay(controller)

    reset(zkClient)
    expect(zkClient.clusterLinkExists(linkId)).andReturn(true)
    expect(zkClient.getAllTopicsInCluster(false)).andReturn(topics)
    expect(zkClient.getClusterLinkForTopics(topicGroups(0))).andReturn(Map(
      topicGroups(0).head -> ClusterLinkTopicState.Mirror(linkName, linkId),
      topicGroups(0).last -> ClusterLinkTopicState.Mirror(otherLinkName, otherLinkId)))
    expect(zkClient.getClusterLinkForTopics(topicGroups(1))).andReturn(Map(
      topicGroups(1).head -> ClusterLinkTopicState.Mirror(linkName, linkId),
      topicGroups(1).last -> ClusterLinkTopicState.FailedMirror(linkName, linkId)))
    expect(zkClient.getClusterLinkForTopics(topicGroups(2))).andReturn(Map.empty)
    replay(zkClient)

    val expectedOps1: List[AlterMirrorsRequest.Op] = List(
      new AlterMirrorsRequest.ClearTopicMirrorOp(topicGroups(0).head))
    val expectedOps2: List[AlterMirrorsRequest.Op] = List(
      new AlterMirrorsRequest.ClearTopicMirrorOp(topicGroups(1).head),
      new AlterMirrorsRequest.ClearTopicMirrorOp(topicGroups(1).last))

    val ret1 = newAlterMirrorsResult(List(
      new AlterMirrorsResponse.ClearTopicMirrorResult()))
    val ret2 = newAlterMirrorsResult(List(
      new AlterMirrorsResponse.ClearTopicMirrorResult(),
      new AlterMirrorsResponse.ClearTopicMirrorResult()))

    reset(admin)
    expect(admin.alterMirrors(EM.eq(expectedOps1.asJava), anyObject())).andReturn(ret1)
    expect(admin.alterMirrors(EM.eq(expectedOps2.asJava), anyObject())).andReturn(ret2)
    replay(admin)

    val done = new CompletableFuture[Void]
    val clearTopicMirrors = newClearTopicMirrors(() => done.complete(null), topicGroupSize)
    clearTopicMirrors.runOnce().get(5, TimeUnit.SECONDS)
    done.get

    verify(admin)
    verify(controller)
    verify(zkClient)
  }

  private def newAlterMirrorsResult(results: Seq[AlterMirrorsResponse.ClearTopicMirrorResult]): AlterMirrorsResult =
    new AlterMirrorsResult(results.map { res =>
      val future = new KafkaFutureImpl[AlterMirrorsResponse.Result]
      future.complete(res)
      future.asInstanceOf[KafkaFuture[AlterMirrorsResponse.Result]]
    }.toList.asJava)

  private def newClearTopicMirrors(completionCallback: () => Unit, topicGroupSize: Int = 10): ClusterLinkClearTopicMirrors =
    new ClusterLinkClearTopicMirrors(linkId, scheduler, zkClient, controller, admin,
      completionCallback, topicGroupSize, intervalMs = 10, retryDelayMs = 10)
}
