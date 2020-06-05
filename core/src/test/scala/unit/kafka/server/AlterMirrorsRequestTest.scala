/*
 * Copyright 2020 Confluent Inc.
 */
package kafka.server

import java.util.Optional
import java.util.concurrent.ExecutionException

import kafka.api.IntegrationTestHarness
import kafka.server.link.ClusterLinkTopicState
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.admin.{Admin, ConfluentAdmin, CreateClusterLinksOptions, CreateTopicsOptions, NewTopic, NewTopicMirror}
import org.apache.kafka.common.KafkaFuture
import org.apache.kafka.common.errors._
import org.apache.kafka.common.internals.KafkaFutureImpl
import org.apache.kafka.common.requests.{AlterMirrorsRequest, AlterMirrorsResponse, NewClusterLink}
import org.junit.Assert._
import org.junit.Test
import org.scalatest.Assertions.intercept

import scala.jdk.CollectionConverters._

class AlterMirrorsRequestTest extends BaseRequestTest {

  override def brokerCount: Int = 1

  @Test
  def testAlterMirrorsEmpty(): Unit = {
    assertTrue(alterMirrors(List.empty).isEmpty)
  }

  @Test(expected = classOf[InvalidRequestException])
  def testAlterMirrorsBadOp(): Unit = {
    alterMirrors(List(new AlterMirrorsRequest.Op {}))
  }

  @Test
  def testStopTopicMirror(): Unit = {
    val topic = "test-topic"
    val localAdmin = createAdminClient()

    runWithRemoteCluster((remoteCluster: IntegrationTestHarness) => {
      val remoteAdmin = remoteCluster.createAdminClient()

      createClusterLinkWith(localAdmin, "test-link", remoteCluster.brokerList)
      createTopicWith(remoteAdmin, topic)
      createTopicWith(localAdmin, topic, Some("test-link"), Some(topic))

      stopTopicMirror(topic)
      intercept[InvalidRequestException] {
        stopTopicMirror(topic)
      }
    })
  }

  @Test
  def testStopTopicMirrorInvalidTopic(): Unit = {
    intercept[InvalidTopicException] {
      stopTopicMirror("topic!")
    }
  }

  @Test
  def testStopTopicMirrorNonexistentTopic(): Unit = {
    intercept[UnknownTopicOrPartitionException] {
      stopTopicMirror("unknown-topic")
    }
  }

  @Test
  def testClearTopicMirror(): Unit = {
    val activeTopic = "active-topic"
    val stoppedTopic = "stopped-topic"
    val localAdmin = createAdminClient()

    def hasClusterLink(topic: String): Boolean = getClusterLinkForTopic(topic).nonEmpty

    runWithRemoteCluster((remoteCluster: IntegrationTestHarness) => {
      val remoteAdmin = remoteCluster.createAdminClient()

      createClusterLinkWith(localAdmin, "test-link", remoteCluster.brokerList)
      createTopicWith(remoteAdmin, activeTopic)
      createTopicWith(localAdmin, activeTopic, Some("test-link"), Some(activeTopic))

      assertTrue(topicIsActiveMirror(activeTopic))
      clearTopicMirror(activeTopic)
      assertFalse(hasClusterLink(activeTopic))
      clearTopicMirror(activeTopic)  // OK, idempotent
      assertFalse(hasClusterLink(activeTopic))

      createTopicWith(remoteAdmin, stoppedTopic)
      createTopicWith(localAdmin, stoppedTopic, Some("test-link"), Some(stoppedTopic))
      assertTrue(topicIsActiveMirror(stoppedTopic))
      stopTopicMirror(stoppedTopic)
      assertTrue(topicIsStoppedMirror(stoppedTopic))
      clearTopicMirror(stoppedTopic)
      assertFalse(hasClusterLink(stoppedTopic))
    })
  }

  @Test
  def testClearTopicMirrorInvalidTopic(): Unit = {
    intercept[InvalidTopicException] {
      clearTopicMirror("topic!")
    }
  }

  @Test
  def testClearTopicMirrorNonexistentTopic(): Unit = {
    intercept[UnknownTopicOrPartitionException] {
      clearTopicMirror("unknown-topic")
    }
  }

  private def runWithRemoteCluster(callback: (IntegrationTestHarness) => Unit): Unit = {
    val remoteCluster = new IntegrationTestHarness() {
      override def brokerCount: Int = 1
    }
    remoteCluster.setUp()
    try {
      callback(remoteCluster)
    } finally {
      remoteCluster.tearDown()
    }
  }

  private def createClusterLinkWith(adminClient: Admin, linkName: String, bootstrapServers: String): Unit = {
    val admin = adminClient.asInstanceOf[ConfluentAdmin]
    val configs = Map(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG -> bootstrapServers)
    val newClusterLink = new NewClusterLink(linkName, null, configs.asJava)
    admin.createClusterLinks(Seq(newClusterLink).asJavaCollection, new CreateClusterLinksOptions().timeoutMs(1000)).all.get
  }

  private def createTopicWith(adminClient: Admin, topic: String, linkName: Option[String] = None, mirrorTopic: Option[String] = None): Unit = {
    val admin = adminClient.asInstanceOf[ConfluentAdmin]
    val newTopic = new NewTopic(topic, Optional.empty[Integer], Optional.of(Short.box(1)))
    linkName.foreach(ln => newTopic.mirror(Optional.of(new NewTopicMirror(ln, mirrorTopic.get))))
    admin.createTopics(Seq(newTopic).asJavaCollection, new CreateTopicsOptions().timeoutMs(1000)).all.get
  }

  private def stopTopicMirror(topic: String): Unit = {
    try {
      val res = alterMirrors(List(new AlterMirrorsRequest.StopTopicMirrorOp(topic)))(0).get
      assertTrue(res.isInstanceOf[AlterMirrorsResponse.StopTopicMirrorResult])
    } catch {
      case e: ExecutionException => throw e.getCause
    }
  }

  private def clearTopicMirror(topic: String): Unit = {
    try {
      val res = alterMirrors(List(new AlterMirrorsRequest.ClearTopicMirrorOp(topic)))(0).get
      assertTrue(res.isInstanceOf[AlterMirrorsResponse.ClearTopicMirrorResult])
    } catch {
      case e: ExecutionException => throw e.getCause
    }
  }

  private def alterMirrors(ops: List[AlterMirrorsRequest.Op],
    validateOnly: Boolean = false, timeoutMs: Int = 5000): List[KafkaFuture[AlterMirrorsResponse.Result]] = {

    val results = ops.map(_ => new KafkaFutureImpl[AlterMirrorsResponse.Result]).toList.asJava
    sendAlterMirrorsRequest(ops, validateOnly, timeoutMs).complete(results)
    assertEquals(ops.size, results.size)
    results.asScala.toList
  }

  private def sendAlterMirrorsRequest(ops: List[AlterMirrorsRequest.Op], validateOnly: Boolean, timeoutMs: Int): AlterMirrorsResponse = {
    val request = new AlterMirrorsRequest.Builder(ops.asJava, validateOnly, timeoutMs).build()
    connectAndReceive[AlterMirrorsResponse](request, destination = controllerSocketServer)
  }

  private def getClusterLinkForTopic(topic: String): Option[ClusterLinkTopicState] =
    zkClient.getClusterLinkForTopics(Set(topic)).get(topic)

  private def topicIsActiveMirror(topic: String): Boolean =
    getClusterLinkForTopic(topic).map(_.isInstanceOf[ClusterLinkTopicState.Mirror]).getOrElse(false)

  private def topicIsStoppedMirror(topic: String): Boolean =
    getClusterLinkForTopic(topic).map(_.isInstanceOf[ClusterLinkTopicState.StoppedMirror]).getOrElse(false)
}
