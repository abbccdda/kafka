/*
 * Copyright 2020 Confluent Inc.
 */
package kafka.server

import java.util.Optional
import java.util.concurrent.ExecutionException

import kafka.api.IntegrationTestHarness
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.admin.{Admin, ConfluentAdmin, CreateClusterLinksOptions, CreateTopicsOptions, NewTopic}
import org.apache.kafka.common.KafkaFuture
import org.apache.kafka.common.errors._
import org.apache.kafka.common.internals.KafkaFutureImpl
import org.apache.kafka.common.requests.{AlterMirrorsRequest, AlterMirrorsResponse, NewClusterLink}
import org.junit.Assert._
import org.junit.Test
import org.scalatest.Assertions.intercept

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

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

      val results1 = alterMirrors(List(new AlterMirrorsRequest.StopTopicMirrorOp(topic)))
      val res = results1(0).get
      assertTrue(res.isInstanceOf[AlterMirrorsResponse.StopTopicMirrorResult])

      val results2 = alterMirrors(List(new AlterMirrorsRequest.StopTopicMirrorOp(topic)))
      interceptExecutionException[InvalidRequestException] {
        results2(0).get
      }
    })
  }

  @Test
  def testStopTopicMirrorInvalidTopic(): Unit = {
    val results = alterMirrors(List(new AlterMirrorsRequest.StopTopicMirrorOp("topic!")))
    interceptExecutionException[InvalidTopicException] {
      results(0).get
    }
  }

  @Test
  def testStopTopicMirrorNonexistentTopic(): Unit = {
    val results = alterMirrors(List(new AlterMirrorsRequest.StopTopicMirrorOp("unknown-topic")))
    interceptExecutionException[UnknownTopicOrPartitionException] {
      results(0).get
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
    linkName.foreach(ln => newTopic.linkName(Optional.of(ln)))
    mirrorTopic.foreach(mt => newTopic.mirrorTopic(Optional.of(mt)))
    admin.createTopics(Seq(newTopic).asJavaCollection, new CreateTopicsOptions().timeoutMs(1000)).all.get
  }

  private def interceptExecutionException[T <: AnyRef : ClassTag](callable: () => Unit): Unit = {
    intercept[T] {
      try {
        callable()
      } catch {
        case e: ExecutionException => throw e.getCause
      }
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

}
