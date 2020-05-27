/*
 * Copyright 2020 Confluent Inc.
 */
package kafka.admin

import joptsimple.OptionException
import kafka.server.link.ClusterLinkConfig
import kafka.utils.TestUtils
import org.apache.kafka.clients.admin.{CreateClusterLinksOptions, CreateClusterLinksResult, DeleteClusterLinksOptions, DeleteClusterLinksResult, ListClusterLinksOptions, ListClusterLinksResult, MockAdminClient}
import org.apache.kafka.common.{KafkaFuture, Node}
import org.apache.kafka.common.internals.KafkaFutureImpl
import org.apache.kafka.common.requests.{ClusterLinkListing, NewClusterLink}
import org.easymock.EasyMock
import org.junit.Assert._
import org.junit.Test
import org.scalatest.Assertions.intercept

import scala.jdk.CollectionConverters._

final class ClusterLinkCommandTest {
  val completed = "successfully completed"
  val validated = "successfully validated"

  private def runCommand(args: Array[String], mockAdminClient: TestAdminClient): String = {
    TestUtils.grabConsoleOutput(ClusterLinkCommand.run(Array("--bootstrap-server", "localhost:9092") ++ args, Some(mockAdminClient)))
  }

  private def createClusterLinks(args: Array[String], expectValidateOnly: Boolean = false, expectValidateLink: Boolean = true): Unit = {
    val linkName = "test-link"
    val clusterId = "test-cluster-id"
    var issuedCommand = false

    val future = new KafkaFutureImpl[Void]
    future.complete(null)

    val result: CreateClusterLinksResult = EasyMock.createNiceMock(classOf[CreateClusterLinksResult])
    EasyMock.expect(result.all()).andReturn(future.asInstanceOf[KafkaFuture[Void]]).once()

    val aclJson: String =
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
    val offsetJson: String =
      """{
        |"groupFilters": [{
        |     "name": "*",
        |     "patternType": "LITERAL",
        |     "filterType": "WHITELIST"
        |  },
        |  {
        |     "name": "blackListed",
        |     "patternType": "PREFIXED",
        |     "filterType": "BLACKLIST"
        |  }]
        | }""".stripMargin

    val configs = Map("bootstrap.servers" -> "10.20.30.40:9092", "request.timeout.ms" -> "100000",
      ClusterLinkConfig.AclSyncEnableProp -> "true", ClusterLinkConfig.AclFiltersProp -> aclJson,
      ClusterLinkConfig.ConsumerOffsetSyncEnableProp -> "true", ClusterLinkConfig.ConsumerOffsetGroupFiltersProp -> offsetJson)

    val node = new Node(1, "localhost", 9092)
    val mockAdminClient = new TestAdminClient(node) {
      override def createClusterLinks(clusterLinks: java.util.Collection[NewClusterLink], options: CreateClusterLinksOptions): CreateClusterLinksResult = {
        issuedCommand = true
        assertEquals(1, clusterLinks.size)
        val newClusterLink = clusterLinks.iterator.next
        assertEquals(linkName, newClusterLink.linkName)
        assertEquals(clusterId, newClusterLink.clusterId)
        assertEquals(configs.asJava, newClusterLink.configs)
        assertEquals(expectValidateOnly, options.validateOnly)
        assertEquals(expectValidateLink, options.validateLink)
        result
      }
    }

    EasyMock.replay(result)
    val output = runCommand(Array("--create", "--link-name", linkName, "--cluster-id", clusterId,
      "--acl-filters-json", aclJson, "--consumer-group-filters-json", offsetJson, "--config", configs.map(kv => kv._1 + "=" + kv._2).mkString(","))
      ++ args, mockAdminClient)
    assertTrue(issuedCommand)
    EasyMock.reset(result)

    val expectedOutput = if (expectValidateOnly) validated else completed
    assertTrue(output.contains(expectedOutput))
  }

  @Test
  def testCreateClusterLinks(): Unit = {
    createClusterLinks(Array.empty)
    createClusterLinks(Array("--validate-only"), expectValidateOnly = true)
    createClusterLinks(Array("--exclude-validate-link"), expectValidateLink = false)
  }

  @Test
  def testListClusterLinks(): Unit = {
    var issuedCommand = false

    val listing = Set(new ClusterLinkListing("link-1", java.util.UUID.randomUUID(), "cluster-id-1"),
      new ClusterLinkListing("link-2", java.util.UUID.randomUUID(), null))
    val future = new KafkaFutureImpl[java.util.Collection[ClusterLinkListing]]
    future.complete(listing.asJava)

    val result: ListClusterLinksResult = EasyMock.createNiceMock(classOf[ListClusterLinksResult])
    EasyMock.expect(result.result()).andReturn(future.asInstanceOf[KafkaFuture[java.util.Collection[ClusterLinkListing]]]).once()

    val node = new Node(1, "localhost", 9092)
    val mockAdminClient = new TestAdminClient(node) {
      override def listClusterLinks(options: ListClusterLinksOptions): ListClusterLinksResult = {
        issuedCommand = true
        result
      }
    }

    EasyMock.replay(result)
    val output = runCommand(Array("--list"), mockAdminClient)
    assertTrue(issuedCommand)
    EasyMock.reset(result)

    listing.foreach { entry =>
      assertTrue(output.contains(entry.linkName))
      assertTrue(output.contains(entry.linkId.toString))
      if (entry.clusterId != null)
        assertTrue(output.contains(entry.clusterId))
    }
  }

  private def deleteClusterLinks(args: Array[String], expectValidateOnly: Boolean = false, expectForce: Boolean = false): Unit = {
    val linkName = "test-link"
    var issuedCommand = false

    val future = new KafkaFutureImpl[Void]
    future.complete(null)

    val result: DeleteClusterLinksResult = EasyMock.createNiceMock(classOf[DeleteClusterLinksResult])
    EasyMock.expect(result.all()).andReturn(future.asInstanceOf[KafkaFuture[Void]]).once()

    val node = new Node(1, "localhost", 9092)
    val mockAdminClient = new TestAdminClient(node) {
      override def deleteClusterLinks(linkNames: java.util.Collection[String], options: DeleteClusterLinksOptions): DeleteClusterLinksResult = {
        issuedCommand = true
        assertEquals(1, linkNames.size)
        assertEquals(linkName, linkNames.iterator.next)
        assertEquals(expectValidateOnly, options.validateOnly)
        assertEquals(expectForce, options.force)
        result
      }
    }

    EasyMock.replay(result)
    val output = runCommand(Array("--delete", "--link-name", linkName) ++ args, mockAdminClient)
    assertTrue(issuedCommand)
    EasyMock.reset(result)

    val expectedOutput = if (expectValidateOnly) validated else completed
    assertTrue(output.contains(expectedOutput))
  }

  @Test
  def testDeleteClusterLinks(): Unit = {
    deleteClusterLinks(Array.empty)
    deleteClusterLinks(Array("--validate-only"), expectValidateOnly = true)
    deleteClusterLinks(Array("--force"), expectForce = true)
  }

  @Test
  def testBadCommands(): Unit = {
    intercept[IllegalArgumentException] {
      new ClusterLinkCommandOptions(Array("--bootstrap-server", "localhost:9092")).verifyArgs()
    }
    intercept[OptionException] {
      new ClusterLinkCommandOptions(Array("--bootstrap-server", "localhost:9092", "--not-a-command")).verifyArgs()
    }
    intercept[IllegalArgumentException] {
      new ClusterLinkCommandOptions(Array("--bootstrap-server", "localhost:9092", "--create", "--list")).verifyArgs()
    }
  }

  @Test
  def testBadConfigs(): Unit = {
    intercept[IllegalArgumentException] {
      new ClusterLinkCommandOptions(Array("--create", "--link-name", "test-link")).verifyArgs()
    }
    intercept[IllegalArgumentException] {
      new ClusterLinkCommandOptions(Array(
        "--create", "--link-name", "test-link",
        "--config", "bootstrap.servers=10.20.30.40:9092",
        "--config-file", "cluster-link.properties")).verifyArgs()
    }
  }

  @Test
  def testMissingRequiredArgs(): Unit = {
    intercept[IllegalArgumentException] {
      new ClusterLinkCommandOptions(Array("--list")).verifyArgs()
    }
    intercept[IllegalArgumentException] {
      new ClusterLinkCommandOptions(Array("--bootstrap-server", "localhost:9092",
        "--create", "--config", "bootstrap.servers=10.20.30.40")).verifyArgs()
    }
    intercept[IllegalArgumentException] {
      new ClusterLinkCommandOptions(Array("--bootstrap-server", "localhost:9092",
        "--create", "--link-name", "test-link")).verifyArgs()
    }
    intercept[IllegalArgumentException] {
      new ClusterLinkCommandOptions(Array("--bootstrap-server", "localhost:9092",
        "--delete")).verifyArgs()
    }
    intercept[IllegalArgumentException] {
      new ClusterLinkCommandOptions(Array("--bootstrap-server", "localhost:9092",
        "--create", "test-link", "--exclude-validate-link")).verifyArgs()
    }
  }

  @Test
  def testInvalidArgs(): Unit = {
    intercept[IllegalArgumentException] {
      new ClusterLinkCommandOptions(Array("--bootstrap-server", "localhost:9092",
        "--create", "--link-name", "test-link", "--config", "bootstrap.servers=10.20.30.40:9092", "--force")).verifyArgs()
    }
    intercept[IllegalArgumentException] {
      new ClusterLinkCommandOptions(Array("--bootstrap-server", "localhost:9092", "--list", "--force")).verifyArgs()
    }
    intercept[IllegalArgumentException] {
      new ClusterLinkCommandOptions(Array("--bootstrap-server", "localhost:9092", "--list", "--cluster-id", "123")).verifyArgs()
    }
    intercept[IllegalArgumentException] {
      new ClusterLinkCommandOptions(Array("--bootstrap-server", "localhost:9092", "--list",
        "--config", "bootstrap.servers=10.20.30.40:9092")).verifyArgs()
    }
    intercept[IllegalArgumentException] {
      new ClusterLinkCommandOptions(Array("--bootstrap-server", "localhost:9092",
        "--delete", "--link-name", "test-link", "--cluster-id", "123")).verifyArgs()
    }
    intercept[IllegalArgumentException] {
      new ClusterLinkCommandOptions(Array("--bootstrap-server", "localhost:9092",
        "--delete", "--link-name", "test-link", "--config", "bootstrap.servers=10.20.30.40:9092")).verifyArgs()
    }
    intercept[IllegalArgumentException] {
      new ClusterLinkCommandOptions(Array("--bootstrap-server", "localhost:9092",
        "--delete", "--link-name", "test-link", "--exclude-validate-link")).verifyArgs()
    }
  }

  class TestAdminClient(node: Node) extends MockAdminClient(java.util.Collections.singletonList(node), node) {
    override def createClusterLinks(clusterLinks: java.util.Collection[NewClusterLink], options: CreateClusterLinksOptions): CreateClusterLinksResult =
      EasyMock.createNiceMock(classOf[CreateClusterLinksResult])
    override def listClusterLinks(options: ListClusterLinksOptions): ListClusterLinksResult =
      EasyMock.createNiceMock(classOf[ListClusterLinksResult])
    override def deleteClusterLinks(linkNames: java.util.Collection[String], options: DeleteClusterLinksOptions): DeleteClusterLinksResult =
      EasyMock.createNiceMock(classOf[DeleteClusterLinksResult])
  }
}
