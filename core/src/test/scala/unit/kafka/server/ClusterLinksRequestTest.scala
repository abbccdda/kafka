/*
 * Copyright 2020 Confluent Inc.
 */
package kafka.server

import java.util.concurrent.ExecutionException

import kafka.api.IntegrationTestHarness
import org.apache.kafka.clients.admin.DescribeClusterOptions
import org.apache.kafka.common.internals.KafkaFutureImpl
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{ClusterLinkListing, CreateClusterLinksRequest, CreateClusterLinksResponse, DeleteClusterLinksRequest, DeleteClusterLinksResponse, ListClusterLinksRequest, ListClusterLinksResponse, NewClusterLink}
import org.apache.kafka.test.TestUtils
import org.junit.Assert._
import org.junit.Test

import scala.jdk.CollectionConverters._

class ClusterLinksRequestTest extends BaseRequestTest {

  override def brokerCount: Int = 1

  @Test
  def testCreateClusterLinks(): Unit = {
    val newClusterLinks = Seq(
      newNewClusterLink("cluster-1", Some("cluster-id-1"), Map.empty[String, String]),
      newNewClusterLink("cluster-2", Some("cluster-id-2"), Map(("request.timeout.ms" -> "10000")))
    )
    val expectedResults = newClusterLinks.map(ncl => (ncl.linkName -> Errors.NONE)).toMap
    val results = createClusterLinks(newClusterLinks, validateOnly = false, validateLink = false)
    assertEquals(expectedResults, results)
    assertClusterLinksEquals(newClusterLinks.map(_.linkName))
  }

  @Test
  def testCreateClusterLinksWithRemote(): Unit = {
    runWithRemoteCluster((remoteBootstrapServers: String, remoteClusterId: Option[String]) => {
      val newClusterLinks = Seq(
        newNewClusterLink("cluster-1", None, Map.empty[String, String], remoteBootstrapServers),
        newNewClusterLink("cluster-2", remoteClusterId, Map.empty[String, String], remoteBootstrapServers),
        newNewClusterLink("cluster-3", Some("bad"), Map.empty[String, String], remoteBootstrapServers)
      )
      val expectedResults = Map(
        "cluster-1" -> Errors.NONE,
        "cluster-2" -> Errors.NONE,
        "cluster-3" -> Errors.INVALID_REQUEST,
      )
      val results = createClusterLinks(newClusterLinks, validateOnly = false, validateLink = true)
      assertEquals(expectedResults, results)
      assertClusterLinksEquals(Seq("cluster-1", "cluster-2"))
    })
  }

  @Test
  def testCreateClusterLinksValidateOnly(): Unit = {
    val newClusterLinks = Seq(newNewClusterLink("cluster", Some("cluster-id"), Map.empty[String, String]))
    val expectedResults = newClusterLinks.map(ncl => (ncl.linkName -> Errors.NONE)).toMap
    val results = createClusterLinks(newClusterLinks, validateOnly = true, validateLink = false)
    assertEquals(expectedResults, results)
    assertClusterLinksEquals(Seq.empty)
  }

  @Test
  def testCreateClusterLinksToSelf(): Unit = {
    val clusterId = createAdminClient().describeCluster(new DescribeClusterOptions().timeoutMs(1000)).clusterId.get
    val newClusterLinks = Seq(newNewClusterLink("cluster", Some(clusterId), Map.empty[String, String]))
    val expectedResults = newClusterLinks.map(ncl => (ncl.linkName -> Errors.INVALID_REQUEST)).toMap
    val results = createClusterLinks(newClusterLinks, validateOnly = true, validateLink = false)
    assertEquals(expectedResults, results)
    assertClusterLinksEquals(Seq.empty)
  }

  @Test
  def testCreateClusterLinksEmptyName(): Unit = {
    val newClusterLinks = Seq(newNewClusterLink("", Some("cluster-id"), Map.empty[String, String]))
    val expectedResults = newClusterLinks.map(ncl => (ncl.linkName -> Errors.INVALID_CLUSTER_LINK)).toMap
    val results = createClusterLinks(newClusterLinks, validateOnly = false, validateLink = false)
    assertEquals(expectedResults, results)
    assertClusterLinksEquals(Seq.empty)
  }

  @Test
  def testCreateClusterLinksInvalidName(): Unit = {
    val newClusterLink = Seq(newNewClusterLink("cluster+1", None, Map.empty[String, String]))
    val expectedResults = newClusterLink.map(ncl => (ncl.linkName -> Errors.INVALID_CLUSTER_LINK)).toMap
    val results = createClusterLinks(newClusterLink, validateOnly = false, validateLink = false)
    assertEquals(expectedResults, results)
    assertClusterLinksEquals(Seq.empty)
  }

  @Test
  def testCreateClusterLinksDuplicateName(): Unit = {
    val newClusterLinks = Seq(
      newNewClusterLink("cluster", Some("cluster-id"), Map.empty[String, String]),
      newNewClusterLink("cluster", Some("cluster-id"), Map(("request.timeout.ms" -> "10000")))
    )
    val expectedResults = newClusterLinks.map(ncl => (ncl.linkName -> Errors.INVALID_REQUEST)).toMap
    val results = createClusterLinks(newClusterLinks, validateOnly = false, validateLink = false)
    assertEquals(expectedResults, results)
    assertClusterLinksEquals(Seq.empty)
  }

  @Test
  def testCreateClusterLinksAlreadyExists(): Unit = {
    val newClusterLink = Seq(newNewClusterLink("cluster", Some("cluster-id"), Map.empty[String, String]))

    val expectedResults1 = newClusterLink.map(ncl => (ncl.linkName -> Errors.NONE)).toMap
    val results1 = createClusterLinks(newClusterLink, validateOnly = false, validateLink = false)
    assertEquals(expectedResults1, results1)
    assertClusterLinksEquals(newClusterLink.map(_.linkName))

    val expectedResults2 = newClusterLink.map(ncl => (ncl.linkName -> Errors.CLUSTER_LINK_EXISTS)).toMap
    val results2 = createClusterLinks(newClusterLink, validateOnly = false, validateLink = false)
    assertEquals(expectedResults2, results2)
    assertClusterLinksEquals(newClusterLink.map(_.linkName))
  }

  @Test
  def testCreateClusterLinksDuplicateClusterId(): Unit = {
    // Should be successful.
    val newClusterLinks = Seq(
      newNewClusterLink("cluster-1", Some("shared-cluster-id"), Map.empty[String, String]),
      newNewClusterLink("cluster-2", Some("shared-cluster-id"), Map(("request.timeout.ms" -> "10000")))
    )
    val expectedResults = newClusterLinks.map(ncl => (ncl.linkName -> Errors.NONE)).toMap
    val results = createClusterLinks(newClusterLinks, validateOnly = false, validateLink = false)
    assertEquals(expectedResults, results)
    assertClusterLinksEquals(newClusterLinks.map(_.linkName))
  }

  @Test
  def testCreateClusterLinksNoClusterId(): Unit = {
    val newClusterLink = Seq(newNewClusterLink("cluster-1", None, Map.empty[String, String]))
    val expectedResults = newClusterLink.map(ncl => (ncl.linkName -> Errors.NONE)).toMap
    val results = createClusterLinks(newClusterLink, validateOnly = false, validateLink = false)
    assertEquals(expectedResults, results)
    assertClusterLinksEquals(newClusterLink.map(_.linkName))
  }

  @Test
  def testCreateClusterLinksEmpty(): Unit = {
    val results = createClusterLinks(Seq(), validateOnly = false, validateLink = false)
    assertEquals(Map.empty, results)
    assertClusterLinksEquals(Seq.empty)
  }

  @Test
  def testCreateClusterLinksRemoteUnreachable(): Unit = {
    val newClusterLinks = Seq(newNewClusterLink("cluster", None, Map.empty[String, String], "doesnt-exist:12345"))
    val expectedResults = newClusterLinks.map(ncl => (ncl.linkName -> Errors.INVALID_CONFIG)).toMap
    val results = createClusterLinks(newClusterLinks, validateOnly = false, validateLink = true, timeoutMs = 1000)
    assertEquals(expectedResults, results)
    assertClusterLinksEquals(Seq.empty)
  }

  @Test
  def testListClusterLinks(): Unit = {
    val linkNames = Seq("cluster-1", "cluster-2")
    createBasicClusterLinks(linkNames)
    val (results, error) = listClusterLinks()
    assertEquals(Errors.NONE, error)
    assertEquals(linkNames.toSet, results.map(_.linkName))
  }

  @Test
  def testListClusterLinksEmpty(): Unit = {
    val expectedResults = (Set.empty, Errors.NONE)
    val results = listClusterLinks()
    assertEquals(expectedResults, results)
  }

  @Test
  def testDeleteClusterLinks(): Unit = {
    val linkNames = Seq("cluster-1", "cluster-2")
    createBasicClusterLinks(linkNames)
    assertClusterLinksEquals(linkNames)
    val expectedResults = linkNames.map(name => (name -> Errors.NONE)).toMap
    val results = deleteClusterLinks(linkNames, validateOnly = false, force = false)
    assertEquals(expectedResults, results)
    assertClusterLinksEquals(Seq.empty)
  }

  @Test
  def testDeleteClusterLinksValidateOnly(): Unit = {
    val linkNames = Seq("cluster-1", "cluster-2")
    createBasicClusterLinks(linkNames)
    assertClusterLinksEquals(linkNames)
    val expectedResults = linkNames.map(name => (name -> Errors.NONE)).toMap
    val results = deleteClusterLinks(linkNames, validateOnly = true, force = false)
    assertEquals(expectedResults, results)
    assertClusterLinksEquals(linkNames)
  }

  @Test
  def testDeleteClusterLinksNonexistent(): Unit = {
    val linkNames = Seq("cluster-nonexistent")
    val expectedResults = linkNames.map(name => (name -> Errors.CLUSTER_LINK_NOT_FOUND)).toMap
    val results = deleteClusterLinks(linkNames, validateOnly = false, force = false)
    assertEquals(expectedResults, results)
    assertClusterLinksEquals(Seq.empty)
  }

  @Test
  def testDeleteClusterLinksRepeat(): Unit = {
    val linkNames = Seq("cluster")
    createBasicClusterLinks(linkNames)
    assertClusterLinksEquals(linkNames)

    val expectedResults1 = linkNames.map(name => (name -> Errors.NONE)).toMap
    val results1 = deleteClusterLinks(linkNames, validateOnly = false, force = false)
    assertEquals(expectedResults1, results1)
    assertClusterLinksEquals(Seq.empty)

    val expectedResults2 = linkNames.map(name => (name -> Errors.CLUSTER_LINK_NOT_FOUND)).toMap
    val results2 = deleteClusterLinks(linkNames, validateOnly = false, force = false)
    assertEquals(expectedResults2, results2)
    assertClusterLinksEquals(Seq.empty)
  }

  @Test
  def testDeleteClusterLinksEmptyName(): Unit = {
    val linkNames = Seq("")
    val expectedResults = linkNames.map(name => (name -> Errors.INVALID_CLUSTER_LINK)).toMap
    val results = deleteClusterLinks(linkNames, validateOnly = false, force = false)
    assertEquals(expectedResults, results)
  }

  @Test
  def testDeleteClusterLinksInvalidName(): Unit = {
    val linkNames = Seq("cluster+1")
    val expectedResults = linkNames.map(name => (name -> Errors.INVALID_CLUSTER_LINK)).toMap
    val results = deleteClusterLinks(linkNames, validateOnly = false, force = false)
    assertEquals(expectedResults, results)
  }

  @Test
  def testDeleteClusterLinksDuplicateName(): Unit = {
    createBasicClusterLinks(Seq("cluster"))
    assertClusterLinksEquals(Seq("cluster"))
    val linkNames = Seq("cluster", "cluster")
    val expectedResults = linkNames.map(name => (name -> Errors.INVALID_REQUEST)).toMap
    val results = deleteClusterLinks(linkNames, validateOnly = false, force = false)
    assertEquals(expectedResults, results)
    assertClusterLinksEquals(Seq("cluster"))
  }

  @Test
  def testClusterLinksDisabled(): Unit = {
    servers.head.shutdown()
    serverConfig.setProperty(KafkaConfig.ClusterLinkEnableProp, "false")
    TestUtils.setFieldValue(servers.head.config, "clusterLinkEnable", false)
    servers.head.startup()

    val linkName = "testLink"
    val newClusterLinks = Seq(newNewClusterLink(linkName, Some("cluster-id-1"), Map.empty[String, String]))
    assertEquals(Map(linkName -> Errors.CLUSTER_AUTHORIZATION_FAILED),
      createClusterLinks(newClusterLinks, validateOnly = false, validateLink = false))
    assertEquals(Map(linkName -> Errors.CLUSTER_AUTHORIZATION_FAILED),
      createClusterLinks(newClusterLinks, validateOnly = true, validateLink = false))

    assertEquals(Map(linkName -> Errors.CLUSTER_AUTHORIZATION_FAILED),
      deleteClusterLinks(Seq(linkName), validateOnly = false, force = true))
    assertEquals(Map(linkName -> Errors.CLUSTER_AUTHORIZATION_FAILED),
      deleteClusterLinks(Seq(linkName), validateOnly = true, force = true))

    assertEquals((Set.empty, Errors.CLUSTER_AUTHORIZATION_FAILED), listClusterLinks())
  }

  private def runWithRemoteCluster(callback: (String, Option[String]) => Unit): Unit = {
    val remoteCluster = new IntegrationTestHarness() {
      override def brokerCount: Int = 1
    }
    remoteCluster.setUp()
    try {
      val clusterId = remoteCluster.createAdminClient().describeCluster(new DescribeClusterOptions().timeoutMs(1000)).clusterId.get
      callback(remoteCluster.brokerList, Option(clusterId))
    } finally {
      remoteCluster.tearDown()
    }
  }

  private def assertClusterLinksEquals(linkNames: Iterable[String]): Unit = {
    val (clusterLinks, error) = listClusterLinks()
    assertEquals(error, Errors.NONE)
    assertEquals(linkNames.toSet, clusterLinks.map(_.linkName).toSet)
  }

  private def newNewClusterLink(linkName: String, clusterId: Option[String], configs: Map[String, String],
    bootstrapServers: String = "localhost:12345"): NewClusterLink = {
    new NewClusterLink(linkName, clusterId.orNull, (configs + ("bootstrap.servers" -> bootstrapServers)).asJava)
  }

  private def createBasicClusterLinks(linkNames: Iterable[String]): Unit = {
    val newClusterLinks = linkNames.map(name => newNewClusterLink(name, None, Map.empty))
    val expectedResults = newClusterLinks.map(ncl => (ncl.linkName -> Errors.NONE)).toMap
    val results = createClusterLinks(newClusterLinks, validateOnly = false, validateLink = false)
    assertEquals(expectedResults, results)
  }

  private def createClusterLinks(newClusterLinks: Iterable[NewClusterLink],
    validateOnly: Boolean, validateLink: Boolean, timeoutMs: Int = 2000): Map[String, Errors] = {

    val tmpResults = newClusterLinks.map(ncl => (ncl.linkName -> new KafkaFutureImpl[Void])).toMap.asJava
    sendCreateClusterLinksRequest(newClusterLinks, validateOnly, validateLink, timeoutMs).complete(tmpResults)
    val results = tmpResults.asScala
    results.map { case (linkName, future) =>
      assertTrue(newClusterLinks.find(_.linkName == linkName).isDefined)
      val error = try {
        future.get
        Errors.NONE
      } catch {
        case e: ExecutionException => Errors.forException(e.getCause)
      }
      (linkName -> error)
    }.toMap
  }

  private def sendCreateClusterLinksRequest(newClusterLinks: Iterable[NewClusterLink],
    validateOnly: Boolean, validateLink: Boolean, timeoutMs: Int): CreateClusterLinksResponse = {

    val request = new CreateClusterLinksRequest.Builder(newClusterLinks.asJavaCollection, validateOnly, validateLink, timeoutMs).build()
    connectAndReceive[CreateClusterLinksResponse](request, destination = controllerSocketServer)
  }

  private def listClusterLinks(): (Set[ClusterLinkListing], Errors) = {
    val tmpResults = new KafkaFutureImpl[java.util.Collection[ClusterLinkListing]]
    sendListClusterLinksRequest().complete(tmpResults)
    try {
      (tmpResults.get.asScala.toSet, Errors.NONE)
    } catch {
      case e: ExecutionException => (Set.empty, Errors.forException(e.getCause))
    }
  }

  private def sendListClusterLinksRequest(): ListClusterLinksResponse = {
    val request = new ListClusterLinksRequest.Builder().build()
    connectAndReceive[ListClusterLinksResponse](request, destination = controllerSocketServer)
  }

  private def deleteClusterLinks(linkNames: Iterable[String],
    validateOnly: Boolean, force: Boolean): Map[String, Errors] = {

    val tmpResults = linkNames.map(name => (name -> new KafkaFutureImpl[Void])).toMap.asJava
    sendDeleteClusterLinksRequest(linkNames, validateOnly, force).complete(tmpResults)
    val results = tmpResults.asScala
    results.map { case (linkName, future) =>
      assertTrue(linkNames.find(_ == linkName).isDefined)
      val error = try {
        future.get
        Errors.NONE
      } catch {
        case e: ExecutionException => Errors.forException(e.getCause)
      }
      (linkName -> error)
    }.toMap
  }

  private def sendDeleteClusterLinksRequest(linkNames: Iterable[String],
    validateOnly: Boolean, force: Boolean): DeleteClusterLinksResponse = {

    val request = new DeleteClusterLinksRequest.Builder(linkNames.asJavaCollection, validateOnly, force).build()
    connectAndReceive[DeleteClusterLinksResponse](request, destination = controllerSocketServer)
  }
}
